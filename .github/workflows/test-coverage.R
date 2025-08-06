# Test coverage script for GitHub Actions
library(covr)

# Generate coverage
cov <- package_coverage(
  quiet = FALSE,
  clean = FALSE,
  install_path = file.path(normalizePath(Sys.getenv("RUNNER_TEMP"), winslash = "/"), "package")
)

# Generate cobertura format for codecov
to_cobertura(cov)

# Print summary
print(cov)