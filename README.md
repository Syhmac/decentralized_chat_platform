# Decentralized chat platform
This project allows users to create and join chat servers in a decentralized manner. Anyone is free to create their
own server and share its address with their users. DCP aims to provide a secure and private way of communication with
lightweight server and client. 

Please note that this project is in a very early stage of development, and you should NOT use it for proper communication
at this point. You are free to test it and provide feedback, but keep in mind that it does not guarantee security
nor privacy at this point.

## Implemented features
- [x] Sending and receiving messages in real-time using a WebSocket connection.
- [x] Logging message timestamps and usernames.
- [x] Simple command-line interface for sending and receiving messages in the test client.

## Planned features
- [ ] Message encryption through SSL.
- [ ] User authentication and management.
- [ ] Persistent message storage.
- [ ] Graphical user interface for the client.
- [ ] Support for multiple chat channels within a server.
- [ ] File sharing capabilities.

## Far plans
- [ ] Mobile application for iOS and Android.
- [ ] Server federation (allowing servers to share chosen channels with each other).
- [ ] Plugin system for servers (aka. bots, commands, etc.)
- [ ] Voice chat support.
- [ ] Permission controlled channels and user roles.

## Installation
To run the server and client you will need to compile the source code first using Cargo. Make sure that you have Rust
installed on your machine. There will be pre-compiled executable releases in the future when the project is more advanced
and ready to be used by the public.

## FaQ
### How to create a server?
You can create a server by running the server executable on your host machine. Server is listening on port 3000.
So far there is no way to change it, other than modifying the source code and compiling the server yourself. Next, you
will want to port forward the port 3000 on your router to allow external connections (no need for this if you want to run
the server only locally). Finally, share your public IP address with your users, so they can connect to your server.

### How to connect to a server?
Right now you have to compile the test client yourself, and include your server's IP address in the source code.
There are plans to add a command-line prompt to enter the server's URL, and the final client app will have an option to
add and save server addresses.

### Is this project secure?
No, not yet. At this point is more of a prototype to test the basic functions. There will be communication encryption in the future.

### Will you integrate this project with the Fediverse?
No. There is no point in integrating a chat platform with the rest of the Fediverse, which os more "social platform" oriented.
There's just no elegant way to integrate a chat with services like Mastodon, PeerTube or Pixelfed. However, there are plans
to add server federation, which will allow servers to share chosen channels with each other (hence the name decentralized).

### Can I contribute to this project?
Yes, feel free to contribute. You can open issues, suggest features or even create a pull request. Just keep in mind that
I will not accept anything until **February 2026**, as this is a part of my University studies and I need to work on it alone for now.

### Is there a roadmap for this project or release day?
Not at this point. This project is being developed in my free time, and I can't estimate how long will it take to reach
each milestone. I will be updating this README once there's more progress and I might create a proper roadmap in the future.

## License
This project in not under any specific license yet. Feel free to contribute, fork etc. However, try to credit the original author where possible.
I will probably create a proper license for it in the future, but you can be sure that it will stay open-source.