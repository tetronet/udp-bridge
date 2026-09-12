# udp-bridge
Allows you to carry abritraty data, that was originally UDP, it will forward it though the tetronet, allows you to make your LAN services available to the public internet and can travel through a superultramegapuper NAT that prevents UDP-hole-punching and even UDP entirely.

### How it works
1) Both sides connect to the tetronet, your ISP and your router will both see an outgoing connection to a server.
2) If ITOT-UDP sees a UDP-packet, it creates a client for the communication with the true UDP-client.
3) ITOT-UDP sends a packet qt="udp_connect" with a random Connection ID over the tetronet.
4) TTOI-UDP creates a client to communicate to the true UDP-server.
5) TTOI-UDP and ITOT-UDP become a transparent bridge between 2 UDP-based systems.
6) If the UDP-server needs a real IP-address of the client, it will successfully fail, because true server will see IP-address of the TTOI-UDP, and true client will see that it's connecting to the ITOT-UDP's IP-address.

Tested with video transmission over the tetronet using SRT protocol, programs - FFMpeg, protocol - MPEG-TS over SRT. No other use cases were tested, it could possibly run other UDP-based protocols, but I'm not sure. Just don't break anything ;)

### Speed
Enough for video streaming, I tested it, got about 12 Mbps.

### How to setup
Create `config.txt` file in the same directory, as the executable file and follow my instructions:

1) Server config
   ```
   server
   <tetronet access server IP or "portName speed" if ciocil>
   <UDP server IP>
   <UDP server port>
   <verbose or unverbose>
   <L2 tetronet protocol, ciocil=physical, virtual=socket.io websocket>
   ```
   Example:
   ```
   server
   wss://data-set.su:3000/
   127.0.0.1
   5000
   unverbose
   virtual
   ```
2) Client config
   ```
   client
   <tetronet access server IP or "portName speed" if ciocil>
   <tetronet address for the server>
   <port, that ITOT-UDP will listen to forward to the tetronet>
   <verbose or unverbose>
   <ping for sending 1 byte ping packets, bigping for sending giant ping packet, a few kb each, singleping for sending exactly one tiny ping packet at the start of the program>
   <L2 tetronet protocol, ciocil=physical, virtual=socket.io websocket>
   <if L2 is ciocil, "serport" will make it work over a serial port, and "tcp" will make it work over a TCP socket>
   ```
   Example:
   ```
   client
   COM101 115200
   <address of the TTOI-UDP>
   5000
   unverbose
   ping
   ciocil
   serport
   ```
   ```
   client
   wss://data-set.su:3000/
   <address of the TTOI-UDP>
   5000
   unverbose
   ping
   virtual
   // doesn't matter what will be here for "virtual", but this line must exist to prevent failing with IndexOutOfRangeException
   ```

### What to do after
Well any app that doesn't care about IP-addresses and works over UDP will probably correctly work over that bridge, but video works only over SRT, and packet_size 1200-1500, otherwise you will overload the tetronet, and reordering will prevent video from working normally, and you will get trash or a black screen on Raw UDP.

### Be careful
UDP-bridge doesn't offer any kind of encryption, auth system or anything like that. TTOI-UDP will connect all ITOT-UDPs to a configured IP and port, so if you configured 127.0.0.1:5000, nobody on the tetronet will be able to use ITOT-UDP to connect to 127.0.0.1:5001 on your computer. But there's no login required. Anybody who knows your TTOI-UDP's tetronet address will be able to connect to your opened-to-the-tetronet service.

### Linux version and other OSes
Don't be lazy, compile it yourself, my internet is like painfully slow, and linux version is like 30 MB. Just create a new project in visual studio and copy all of the code, it's gonna work if you run these command on your project:
```
dotnet add package SocketIOClient
dotnet add package System.IO.Ports
dotnet add package System.IO.Hashing
```
Nothing more is really needed
