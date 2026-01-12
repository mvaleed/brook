The "Uncertainties" You Will Face
Since you are building a lightweight version, here are the specific traps to avoid:

The "Flush" Trap:
Do not fsync to disk after every single message.
It will destroy performance.
You must rely on the OS to batch writes to the physical disk, or implement an application-side buffer.

The Copy Trap:
Avoid copying data from kernel space to user space and back.
Look into sendfile (Zero Copy) for sending data from disk to the network socket.

The Format Trap:
If you are building your own client, you can design a simpler binary protocol.

By keeping the architecture "Shared-Nothing" (each partition is owned by one CPU core/thread),
you can eventually scale this easily.
But for now, treat the file system as your source of truth.
