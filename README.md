# lightweight-jobs
Set of lightweight HySDS system jobs

# HySDS system jobs
- job retry
- job purge
- notify by email

## Building and Releasing (v2.0.2+)

Starting with release v2.0.2, the lightweight-jobs container supports both x86_64 (amd64) and ARM64 architectures. Due to the combined size of both architecture-specific Docker images, the SDS package exceeds GitHub's 2GB release asset limit and must be distributed via Artifactory or S3.

### Manual Build and Release Process

#### 1. Build the Multi-Architecture Container

Trigger a Jenkins build with `BUILD_MODE=multi-platform` (default):

```bash
# Add Jenkins job
sds ci add_job -b <branch/tag> --pipeline --token https://github.com/hysds/lightweight-jobs.git s3

# Build the container (creates both amd64 and arm64 images)
sds ci build_job -b <branch/tag> https://github.com/hysds/lightweight-jobs.git

# Remove Jenkins job after building
sds ci remove_job -b <branch/tag> https://github.com/hysds/lightweight-jobs.git
```

#### 2. Export the SDS Package

From your Mozart instance:

```bash
sds pkg export container-hysds_lightweight-jobs:<tag> -o /tmp
```

This creates a multi-architecture package containing both x86_64 and ARM64 Docker images.

#### 3. Split and Upload to GitHub Release

Due to GitHub's 2GB asset limit, split the package into chunks:

```bash
cd /tmp
# Split into 1.5GB chunks (well under the 2GB limit)
split -b 1500M container-hysds_lightweight-jobs-<tag>.sdspkg.tar \
  container-hysds_lightweight-jobs-<tag>.sdspkg.tar.part-
```

This creates files like:
- `container-hysds_lightweight-jobs-<tag>.sdspkg.tar.part-aa`
- `container-hysds_lightweight-jobs-<tag>.sdspkg.tar.part-ab`
- `container-hysds_lightweight-jobs-<tag>.sdspkg.tar.part-ac`

**Installing GitHub CLI (if needed):**

```bash
# For x86_64 Linux
curl -fsSL https://github.com/cli/cli/releases/download/v2.40.1/gh_2.40.1_linux_amd64.tar.gz -o gh.tar.gz
tar -xzf gh.tar.gz
sudo mv gh_2.40.1_linux_amd64/bin/gh /usr/local/bin/

# For ARM64 Linux
curl -fsSL https://github.com/cli/cli/releases/download/v2.40.1/gh_2.40.1_linux_arm64.tar.gz -o gh.tar.gz
tar -xzf gh.tar.gz
sudo mv gh_2.40.1_linux_arm64/bin/gh /usr/local/bin/

# Authenticate
gh auth login
```

**Upload using GitHub CLI:**

```bash
gh release upload <tag> container-hysds_lightweight-jobs-<tag>.sdspkg.tar.part-* \
  --repo hysds/lightweight-jobs
```

**Alternative: Upload via GitHub API (without CLI)**

```bash
source ~/.git_oauth_token
owner=hysds
repo=lightweight-jobs
release_id=$(curl -s -H "Authorization: token $GIT_OAUTH_TOKEN" \
  "https://api.github.com/repos/${owner}/${repo}/releases/tags/<tag>" \
  | grep '^  "id":' | awk '{print $2}' | sed 's/,//')

for file in container-hysds_lightweight-jobs-<tag>.sdspkg.tar.part-*; do
  echo "Uploading ${file}..."
  curl -H "Authorization: token $GIT_OAUTH_TOKEN" \
    -H "Content-Type: application/octet-stream" \
    --data-binary @${file} \
    "https://uploads.github.com/repos/${owner}/${repo}/releases/${release_id}/assets?name=$(basename ${file})"
done
```

#### 4. Alternative: Upload to Artifactory or S3

**Option A: Artifactory (For JPL/IEMS-SDS)**

```bash
curl -u <user>:<token> -T container-hysds_lightweight-jobs-<tag>.sdspkg.tar \
  "<artifactory-url>/gov/nasa/jpl/iems/sds/pcm/lightweight-jobs/<tag>/"
```

**Option B: S3 (For Public Access)**

```bash
aws s3 cp container-hysds_lightweight-jobs-<tag>.sdspkg.tar \
  s3://your-bucket/hysds-packages/lightweight-jobs/<tag>/ --acl public-read
```

### Installation

The HySDS framework installation automatically downloads and reassembles chunked packages from GitHub releases.

**Manual Installation:**

If downloading manually, reassemble the parts first:

```bash
# Download all parts
gh release download <tag> --pattern "container-hysds_lightweight-jobs-*.sdspkg.tar.part-*" \
  --repo hysds/lightweight-jobs

# Reassemble
cat container-hysds_lightweight-jobs-<tag>.sdspkg.tar.part-* > \
  container-hysds_lightweight-jobs-<tag>.sdspkg.tar

# Import
sds pkg import container-hysds_lightweight-jobs-<tag>.sdspkg.tar
```

The package contains both x86_64 and ARM64 Docker images. At runtime, Verdi workers will automatically download and use the appropriate architecture-specific image for their platform.

### Why the Change?

The addition of ARM64 support means the multi-architecture SDS package contains Docker images for both x86_64 and ARM64 architectures. The total size exceeds 4GB, which is well beyond GitHub's 2GB release asset limit. By hosting on Artifactory/S3, we can support both architectures while working within platform constraints. The HySDS framework automatically handles architecture selection at runtime.

