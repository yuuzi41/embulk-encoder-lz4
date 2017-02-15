# Lz4 encoder plugin for Embulk

This plugin is embulk file encoder for LZ4, a Extremely fast compression algorithm.

**Caution!** : This plugin supports [LZ4 Frame Format](https://github.com/lz4/lz4/wiki/lz4_Frame_format.md) only!

_LZ4 Frame Format_ is the most famously format for compressing file of any size by LZ4. if you use Linux, `lz4` program has generating a compressed file formatted by _LZ4 Frame Format_.

then, this plugin is not supported other formats based on LZ4 algorithm such as raw _LZ4 Block Format_, currently.

## Overview

* **Plugin type**: encoder

## Configuration

- **block_size**: Block Maximum Size for uncompressed buffer. you can choose from 65535, 262144, 1048576 or 4194304. (integer, default: `4194304`)

## Example

```yaml
out:
  type: any output input plugin type
  encoders:
    - type: lz4
      block_size: 4194304
```

## Build

```
$ ./gradlew gem  # -t to watch change of files and rebuild continuously
```
