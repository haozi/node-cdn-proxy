# 使用官方 Node.js 镜像作为基础镜像
FROM node:22-alpine

# 设置工作目录
WORKDIR /app

# 设置 SHELL 环境变量
ENV SHELL=/bin/bash

# 设置 PNPM_HOME 环境变量
ENV PNPM_HOME=/root/.local/share/pnpm

# 设置全局 bin 目录，并确保它在 PATH 中
ENV PATH=$PNPM_HOME:$PATH

ADD . /app/

# 安装 pnpm 和 vite
RUN apk add --no-cache git && git reset --hard && git clean -xdf && rm -rf .git && \
    CI=true npm i -g pnpm && pnpm i && pnpm build && \
    pnpm cache delete && pnpm store prune && apk del git && rm -rf $PNPM_HOME/store
