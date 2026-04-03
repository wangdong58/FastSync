# 部署指南

## 快速部署（推荐）

### 1. 构建静态二进制文件

```bash
make build-static
```

生成的 `build/sync-tool-static` 是**零依赖**的静态二进制文件。

### 2. 部署到新服务器

```bash
# 复制二进制文件
scp build/sync-tool-static user@new-server:/usr/local/bin/sync-tool

# 复制配置文件
scp config.yaml user@new-server:/etc/sync-tool/config.yaml

# 登录新服务器运行
ssh user@new-server
sync-tool -c /etc/sync-tool/config.yaml
```

## 部署选项对比

| 方案 | 命令 | 优点 | 缺点 |
|------|------|------|------|
| **静态编译** | `make build-static` | 零依赖，任何 Linux x64 都能运行 | 文件稍大 (~15MB) |
| 动态编译 | `make build` | 文件较小 | 需要目标服务器有 glibc |
| Docker | `make docker-build` | 完全隔离，环境一致 | 需要 Docker 环境 |

## 系统要求

### 静态二进制文件
- **OS**: Linux x86_64 (AMD64)
- **依赖**: 无

### 动态二进制文件
- **OS**: Linux x86_64
- **依赖**: glibc 2.17+ (可用 `ldd --version` 检查)

## 完整部署脚本

```bash
#!/bin/bash
# deploy.sh - 一键部署脚本

BINARY_NAME="sync-tool"
DEPLOY_HOST="your-server-ip"
DEPLOY_USER="root"
DEPLOY_DIR="/opt/sync-tool"

# 1. 构建静态二进制文件
echo "Building static binary..."
make build-static

# 2. 创建远程目录
ssh ${DEPLOY_USER}@${DEPLOY_HOST} "mkdir -p ${DEPLOY_DIR}"

# 3. 复制文件
echo "Copying files to server..."
scp build/sync-tool-static ${DEPLOY_USER}@${DEPLOY_HOST}:${DEPLOY_DIR}/sync-tool
scp config/example.yaml ${DEPLOY_USER}@${DEPLOY_HOST}:${DEPLOY_DIR}/config.yaml

# 4. 设置权限
ssh ${DEPLOY_USER}@${DEPLOY_HOST} "chmod +x ${DEPLOY_DIR}/sync-tool"

# 5. 创建软链接
ssh ${DEPLOY_USER}@${DEPLOY_HOST} "ln -sf ${DEPLOY_DIR}/sync-tool /usr/local/bin/sync-tool"

echo "Deployment complete!"
echo "Next steps:"
echo "  1. Edit config: ssh ${DEPLOY_USER}@${DEPLOY_HOST} 'vi ${DEPLOY_DIR}/config.yaml'"
echo "  2. Run sync: ssh ${DEPLOY_USER}@${DEPLOY_HOST} 'sync-tool -c ${DEPLOY_DIR}/config.yaml'"
```

## 环境变量配置

生产环境建议使用环境变量存储密码：

```bash
# /etc/sync-tool/env
export SOURCE_DB_PASSWORD="your_source_password"
export TARGET_DB_PASSWORD="your_target_password"
```

然后在 config.yaml 中引用：
```yaml
source:
  password: "${SOURCE_DB_PASSWORD}"

target:
  password: "${TARGET_DB_PASSWORD}"
```

运行时加载环境变量：
```bash
source /etc/sync-tool/env
sync-tool -c /etc/sync-tool/config.yaml
```

## Systemd 服务（可选）

创建 `/etc/systemd/system/sync-tool.service`：

```ini
[Unit]
Description=SQL Server to OceanBase Sync Tool
After=network.target

[Service]
Type=oneshot
User=syncuser
Environment="SOURCE_DB_PASSWORD=xxx"
Environment="TARGET_DB_PASSWORD=xxx"
ExecStart=/usr/local/bin/sync-tool -c /etc/sync-tool/config.yaml
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
```

启用服务：
```bash
systemctl daemon-reload
systemctl enable sync-tool
systemctl start sync-tool
```

查看日志：
```bash
journalctl -u sync-tool -f
```

## 验证部署

```bash
# 检查二进制是否可用
sync-tool -v

# 检查配置文件
sync-tool -c /etc/sync-tool/config.yaml -v

# 测试数据库连接（先不执行同步）
# 查看配置是否正确加载
```

## 常见问题

### Q: 目标服务器是 ARM 架构怎么办？
```bash
# 交叉编译 ARM64 版本
make build-linux-arm64
```

### Q: 目标服务器是 CentOS 7？
静态编译的二进制文件可以在 CentOS 7+ 上直接运行，无需额外依赖。

### Q: 如何减小二进制文件大小？
```bash
# 使用 upx 压缩
go build -ldflags="-s -w" -o sync-tool ./cmd/sync
upx sync-tool
```
