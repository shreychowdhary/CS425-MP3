.PHONY: all server client clean

client:
	go build Client/client.go
server:
	go build Server/server.go Server/types.go