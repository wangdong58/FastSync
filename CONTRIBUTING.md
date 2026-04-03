# Contributing Guide

Thank you for your interest in contributing to SQL Server to OceanBase MySQL Sync Tool!

## How to Contribute

### Reporting Issues

- Use the [GitHub Issues](https://github.com/yourusername/FastSync/issues) page
- Describe the issue clearly with steps to reproduce
- Include your environment information (OS, Go version, database versions)
- Add relevant log snippets if applicable

### Submitting Pull Requests

1. Fork the repository
2. Create a new branch for your feature/fix
3. Make your changes
4. Run tests and ensure they pass
5. Update documentation if needed
6. Submit a Pull Request

### Development Setup

```bash
# Clone your fork
git clone https://github.com/yourusername/FastSync.git
cd FastSync

# Install dependencies
go mod download

# Build
make build

# Run tests
make test

# Format code
make fmt
```

### Code Style

- Follow standard Go conventions
- Use `gofmt` to format your code
- Keep functions focused and small
- Add comments for exported functions
- Write meaningful commit messages

### Testing

- Add tests for new features
- Ensure existing tests pass
- Test with both SQL Server and MySQL/OceanBase

### Documentation

- Update README.md if adding new features
- Update example.yaml for new configuration options
- Add comments to code changes

## Code of Conduct

- Be respectful and inclusive
- Focus on constructive feedback
- Help others learn and grow

Thank you for contributing!
