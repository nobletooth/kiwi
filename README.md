# Kiwi 🥝

----
Kiwi is a recreational dead-simple LSM-Tree based key-value store like level DB.
<br>
Just fucking around to make some cheap alternative to Redis cluster / DragonflyDB on disk.
Kiwi has a standalone server that supports RESP protocol and is compatible with Redis clients.
---


### Build
To start building Kiwi from source code, you need to have [buf](https://buf.build/docs/cli/installation/), [protoc](https://pkg.go.dev/github.com/golang/protobuf), and Go 1.25 installed on your machine.
```bash
git clone git@github.com:nobletooth/kiwi.git
cd kiwi
make bin/kiwi
```
To build docker image you can do:
```bash
make image
```

---
### Run
To run Kiwi binary directly, you can do:
```bash
./bin/kiwi --data_dir ./data --address 0.0.0.0:6380
```
Kiwi is configured in two ways:
1. Command line flags. Run `./bin/kiwi --help` to see all available flags.
2. Config .txtpb files, passed by `--config_file` flag. Defaults are available at [default.txtpb](pkg/config/config.txtpb). 
<br>
Note that the .txtpb config file overrides flag values.

After your server is up and running, you can connect to it using any Redis client, for example:
```bash
redis-cli -p 6380

SET kiwi fruit
(ok)
GET apple
(nil)
GET kiwi
fruit
```

---
### Test
To run tests, you can do:
```bash
make test
```

---
### Coming soon
I keep a TODO list of the next things I'm going to implement.
<br>
You can check them out [here](TODO.md).