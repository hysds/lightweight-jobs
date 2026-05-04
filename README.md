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

#### 3. Upload to Artifactory or S3

**Option A: Artifactory (Recommended for JPL/IEMS-SDS)**

```bash
curl -u <user>:<token> -T container-hysds_lightweight-jobs-<tag>.sdspkg.tar \
  "<artifactory-url>/gov/nasa/jpl/iems/sds/pcm/lightweight-jobs/<tag>/"
```

**Option B: S3 (For Public Access)**

```bash
aws s3 cp container-hysds_lightweight-jobs-<tag>.sdspkg.tar \
  s3://your-bucket/hysds-packages/lightweight-jobs/<tag>/ --acl public-read
```

#### 4. Create GitHub Release (Optional)

For releases prior to v2.0.2, single-architecture packages were uploaded to GitHub releases. Starting with v2.0.2, packages are distributed via Artifactory or S3 due to size constraints. You can still create a GitHub release with release notes and links to the download location.

### Installation

Import the multi-architecture package on your Mozart instance:

```bash
sds pkg import container-hysds_lightweight-jobs-<tag>.sdspkg.tar
```

The package contains both x86_64 and ARM64 Docker images. At runtime, Verdi workers will automatically download and use the appropriate architecture-specific image for their platform.

### Why the Change?

The addition of ARM64 support means the multi-architecture SDS package contains Docker images for both x86_64 and ARM64 architectures. The total size exceeds 4GB, which is well beyond GitHub's 2GB release asset limit. By hosting on Artifactory/S3, we can support both architectures while working within platform constraints. The HySDS framework automatically handles architecture selection at runtime.

