# Cryptowatch Go SDK Sapientia fork

The Cryptowatch Go SDK can be used to access both the REST and WebSocket APIs. Trading over WebSockets is in beta.

Documentation: https://godoc.org/code.cryptowat.ch/cw-sdk-go

## How to clone

To avoid problems with forked go dependencies the following cloning procedure is recommended:

1. clone the original repo with go get (this ensures that the go workspace dependencies are compatible):
```
go get code.cryptowat.ch/cw-sdk-go
```
2. change git remote to our fork:
```
git remote set-url origin git@github.com:1sapientia/cw-sdk-go.git
```
3. pull our changes:
```
git pull
```

## API documentation:

- REST API: https://cryptowat.ch/docs/api
- WebSocket API: https://cryptowat.ch/docs/websocket-api

## License

[BSD-2-Clause](LICENSE)
