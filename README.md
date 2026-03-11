[中文文档](readme-zh.md) / [English Document](README.md)

# Custom Image Gateway

<p align="center">
    <img src="https://img.shields.io/github/release/haierkeys/custom-image-gateway" alt="version">
    <img src="https://img.shields.io/github/license/haierkeys/custom-image-gateway" alt="license">
</p>

This project provides image upload, storage, and cloud synchronization services for the [Obsidian Custom Image Auto Uploader](https://github.com/haierkeys/obsidian-custom-image-auto-uploader) plugin.

## Features

- [x] Image Upload
- [x] Token Authorization for improved API security
- [x] Image HTTP Access (Basic feature, Nginx is recommended for production)
- [x] Storage Support:
  - [x] Save to both local and cloud storage for easy migration
  - [x] Local Storage Support (Tested, suitable for NAS)
  - [x] Alibaba Cloud OSS Support (Implemented, not yet tested)
  - [x] Cloudflare R2 Support (Implemented and tested)
  - [x] Amazon S3 Support (Implemented and tested)
  - [x] MinIO Storage Support (v1.5+)
  - [x] WebDAV Storage Support (v2.5+)
  - [x] DogeCloud Storage Support (v2.6+)
- [x] Docker Support for easy deployment on home NAS or remote servers
- [x] Public Service API & Web Interface <a href="#userapi">User Public Interface & Web Interface</a>

## Changelog

For the full changelog, please visit [Changelog](https://github.com/haierkeys/custom-image-gateway/releases).

## Pricing

This software is open source and free. If you'd like to express your gratitude or support continued development, you can do so via:

[<img src="https://cdn.ko-fi.com/cdn/kofi3.png?v=3" alt="BuyMeACoffee" width="100">](https://ko-fi.com/haierkeys)

## Quick Start

### Installation

- **Directory Setup**

  ```bash
  # Create directories required for the project
  mkdir -p /data/image-api
  cd /data/image-api

  mkdir -p ./config && mkdir -p ./storage/logs && mkdir -p ./storage/uploads
  ```

  On the first run, if no configuration file is found, the program will automatically generate a default configuration at **config/config.yaml**.

  If you want to download a default configuration from the internet, use the following command:

  ```bash
  # Download default configuration file to the config directory
  wget -P ./config/ https://raw.githubusercontent.com/haierkeys/custom-image-gateway/main/config/config.yaml
  ```

- **Binary Installation**

  Download the latest version from [Releases](https://github.com/haierkeys/custom-image-gateway/releases), unzip it, and run:

  ```bash
  ./image-api run -c config/config.yaml
  ```

- **Containerized Installation (Docker)**

  Docker Command:

  ```bash
  # Pull the latest image
  docker pull haierkeys/custom-image-gateway:latest

  # Create and start the container
  docker run -tid --name image-api \
          -p 9000:9000 -p 9001:9001 \
          -v /data/image-api/storage/:/api/storage/ \
          -v /data/image-api/config/:/api/config/ \
          haierkeys/custom-image-gateway:latest
  ```

  Docker Compose
  Use *containrrr/watchtower* to monitor the image for automatic updates.
  **docker-compose.yaml** content:

  ```yaml
  # docker-compose.yaml
  services:
    image-api:
      image: haierkeys/custom-image-gateway:latest  # Your application image
      container_name: image-api
      ports:
        - "9000:9000"  # Port mapping 9000
        - "9001:9001"  # Port mapping 9001
      volumes:
        - /data/image-api/storage/:/api/storage/  # Storage directory mapping
        - /data/image-api/config/:/api/config/    # Config directory mapping
      restart: always

  ```

  Execute **docker compose**:

  Register docker container as a service:

  ```bash
  docker compose up -d
  ```

  Stop and remove docker container:

  ```bash
  docker compose down
  ```

### Usage

- **Using Single Service Gateway**

	Supports `Local Storage`, `OSS`, `Cloudflare R2`, `Amazon S3`, `MinIO`, `WebDAV`, `DogeCloud`.

	You need to modify [config.yaml](config/config.yaml#http-port).

	Modify `http-port` and `auth-token` options.

	Start the gateway program.

	API Gateway Address: `http://{IP:PORT}/api/upload`

	API Access Token: content of `auth-token`

- **Using Multi-User Open Gateway**

	Supports `Local Storage`, `OSS`, `Cloudflare R2`, `Amazon S3`, `MinIO` (v2.3+), `WebDAV` (v2.5+), `DogeCloud` (v2.6+).

	You need to modify [config.yaml](config/config.yaml#user).

	Modify `http-port` and `database`.

	Also change `user.is-enable` and `user.register-is-enable` to `true`.

	Start the gateway program.

	Access `WebGUI` address `http://{IP:PORT}` for user registration and configuration.

	![Image](https://github.com/user-attachments/assets/39c798de-b243-42c1-a75a-cd179913fc49)

	API Gateway Address: `http://{IP:PORT}/api/user/upload`

	Click on `WebGUI` -> Copy API Config to get configuration information.

- **Storage Type Explanation**

  | Storage Type         | Description                                                                                                                                                                                                                                                                                                                                                                                                  |
  |----------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
  | Local Server Storage | Default save path: `/data/storage/uploads`. Config item `config.local-fs.save-path` is `storage/uploads`. <br />If using gateway image resource access service, set `config.local-fs.httpfs-is-enable` to `true`. <br /> Corresponding `Access Address Prefix` is `http://{IP:PORT}`. For single service gateway, set `config.app.upload-url-pre`. <br />It is recommended to use Nginx for resource access. |

### Configuration

The default configuration file name is **config.yaml**. Please place it in the **root directory** or **config** directory.

For more configuration details, please refer to:

- [config/config.yaml](config/config.yaml)

## Other Resources

- [Obsidian Custom Image Auto Uploader](https://github.com/haierkeys/obsidian-custom-image-auto-uploader)