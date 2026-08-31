# Production secrets

不要把真实密码提交到 Git。服务器上建议保存为 `/etc/fable/secrets.env`，权限设为 `600`。

GitHub Actions 中使用以下 Repository secrets：

- `DEPLOY_HOST=38.147.170.246`
- `DEPLOY_USER`
- `DEPLOY_SSH_KEY`
- `DEPLOY_ENV_FILE`（可选；更推荐在服务器预置 secrets.env）

MySQL、Redis 和 JWT 密钥只通过服务器文件或 Kubernetes Secret 注入。
