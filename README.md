# 競艇投票 _(boatrace-vote)_

[![build](https://github.com/u6k/boatrace-vote/actions/workflows/build.yml/badge.svg)](https://github.com/u6k/boatrace-vote/actions/workflows/build.yml)
[![license](https://img.shields.io/github/license/u6k/boatrace-vote.svg)](https://github.com/u6k/boatrace-vote/blob/master/LICENSE)
[![GitHub release](https://img.shields.io/github/release/u6k/boatrace-vote.svg)](https://github.com/u6k/boatrace-vote/releases)
[![WebSite](https://img.shields.io/website-up-down-green-red/https/shields.io.svg?label=u6k.Redmine)](https://redmine.u6k.me/projects/boatrace-vote)
[![standard-readme compliant](https://img.shields.io/badge/readme%20style-standard-brightgreen.svg?style=flat-square)](https://github.com/RichardLitt/standard-readme)

> 競艇に投票する

## Background

競艇の予測モデルによる高精度の予測と効率的な舟券選択ができれば、自動的に投票すべき舟券と投票量を決定できる。

本ソフトウェアでは、そうやって決定した舟券と投票量に従って、実際に投票する。

## Install

Dockerとdocker composeを使用する。

```bash
$ docker version
Client: Docker Engine - Community
 Version:           24.0.2
 API version:       1.43
 Go version:        go1.20.4
 Git commit:        cb74dfc
 Built:             Thu May 25 21:51:00 2023
 OS/Arch:           linux/amd64
 Context:           default

Server: Docker Engine - Community
 Engine:
  Version:          24.0.2
  API version:      1.43 (minimum version 1.12)
  Go version:       go1.20.4
  Git commit:       659604f
  Built:            Thu May 25 21:51:00 2023
  OS/Arch:          linux/amd64
  Experimental:     false
 containerd:
  Version:          1.6.21
  GitCommit:        3dce8eb055cbb6872793272b4f20ed16117344f8
 runc:
  Version:          1.1.7
  GitCommit:        v1.1.7-0-g860f061
 docker-init:
  Version:          0.19.0
  GitCommit:        de40ad0
```

```bash
$ docker compose version
Docker Compose version v2.18.1
```

ビルド済みDockerイメージを使用する場合、`docker pull`する。

```bash
docker pull ghcr.io/u6k/boatrace-vote
```

Dockerイメージをビルドする場合、docker composeでbuildする。

```bash
docker compose build
```

Linuxに直接セットアップする場合、`Dockerfile`を参照すること。

## Usage

シェルを起動する。

```bash
docker compose run app bash
```

poeコマンドを実行する。例えば、レース一覧を生成する。

```bash
docker compose run app poe create_racelist
```

## Other

最新の情報は、[Wiki - boatrace-vote - u6k.Redmine](https://redmine.u6k.me/projects/boatrace-vote/wiki)を参照してください。

## Maintainer

- u6k
    - [Twitter](https://twitter.com/u6k_yu1)
    - [GitHub](https://github.com/u6k)
    - [Blog](https://blog.u6k.me/)

## Contributing

当プロジェクトに興味を持っていただき、ありがとうございます。[既存のチケット](https://redmine.u6k.me/projects/boatrace-vote/issues)をご覧ください。

当プロジェクトは、[Contributor Covenant](https://www.contributor-covenant.org/version/2/1/code_of_conduct/)に準拠します。

## License

[MIT License](https://github.com/u6k/boatrace-vote/blob/main/LICENSE)
