using System;
using System.Collections.Generic;
using UnityEngine;

namespace Mirror
{
    // a server's connection TO a LocalClient.
    // sending messages on this connection causes the client's handler function to be invoked directly
    public class LocalConnectionToClient : NetworkConnectionToClient
    {
        internal LocalConnectionToServer connectionToServer;

        public LocalConnectionToClient() : base(LocalConnectionId, false) {}

        public override string address => "localhost";

        internal override void Send(ArraySegment<byte> segment, int channelId = Channels.Reliable)
        {
            // get a writer to copy the message into since the segment is only
            // valid until returning.
            // => pooled writer will be returned to pool when dequeuing.
            // => WriteBytes instead of WriteArraySegment because the latter
            //    includes a 4 bytes header. we just want to write raw.
            //Debug.Log("Enqueue " + BitConverter.ToString(segment.Array, segment.Offset, segment.Count));
            PooledNetworkWriter writer = NetworkWriterPool.GetWriter();
            writer.WriteBytes(segment.Array, segment.Offset, segment.Count);
            connectionToServer.queue.Enqueue(writer);
        }

        // true because local connections never timeout
        internal override bool IsAlive(float timeout) => true;

        internal void DisconnectInternal()
        {
            // set not ready and handle clientscene disconnect in any case
            // (might be client or host mode here)
            isReady = false;
            RemoveFromObservingsObservers();
        }

        /// <summary>Disconnects this connection.</summary>
        public override void Disconnect()
        {
            DisconnectInternal();
            connectionToServer.DisconnectInternal();
        }
    }

    // a localClient's connection TO a server.
    // send messages on this connection causes the server's handler function to be invoked directly.
    public class LocalConnectionToServer : NetworkConnectionToServer
    {
        internal LocalConnectionToClient connectionToClient;

        // packet queue
        internal readonly Queue<PooledNetworkWriter> queue = new Queue<PooledNetworkWriter>();

        public override string address => "localhost";

        // see caller for comments on why we need this
        bool connectedEventPending;
        bool disconnectedEventPending;
        internal void QueueConnectedEvent() => connectedEventPending = true;
        internal void QueueDisconnectedEvent() => disconnectedEventPending = true;

        // parameterless constructor that disables batching for local connections
        public LocalConnectionToServer() : base(false) {}

        internal override void Send(ArraySegment<byte> segment, int channelId = Channels.Reliable)
        {
            if (segment.Count == 0)
            {
                Debug.LogError("LocalConnection.SendBytes cannot send zero bytes");
                return;
            }

            // handle the server's message directly
            NetworkServer.OnTransportData(connectionId, segment, channelId);
        }

        internal override void Update()
        {
            base.Update();

            // should we still process a connected event?
            if (connectedEventPending)
            {
                connectedEventPending = false;
                NetworkClient.OnConnectedEvent?.Invoke();
            }

            // process internal messages so they are applied at the correct time
            while (queue.Count > 0)
            {
                // call receive on queued writer's content, return to pool
                PooledNetworkWriter writer = queue.Dequeue();
                ArraySegment<byte> segment = writer.ToArraySegment();
                //Debug.Log("Dequeue " + BitConverter.ToString(segment.Array, segment.Offset, segment.Count));
                NetworkClient.OnTransportData(segment, Channels.Reliable);
                NetworkWriterPool.Recycle(writer);
            }

            // should we still process a disconnected event?
            if (disconnectedEventPending)
            {
                disconnectedEventPending = false;
                NetworkClient.OnDisconnectedEvent?.Invoke();
            }
        }

        /// <summary>Disconnects this connection.</summary>
        internal void DisconnectInternal()
        {
            // set not ready and handle clientscene disconnect in any case
            // (might be client or host mode here)
            // TODO remove redundant state. have one source of truth for .ready!
            isReady = false;
            NetworkClient.ready = false;
        }

        /// <summary>Disconnects this connection.</summary>
        public override void Disconnect()
        {
            connectionToClient.DisconnectInternal();
            DisconnectInternal();

            // this was in NetworkClient.Disconnect 'if isLocalConnection' before
            // but it's clearly local connection related, so put it in here.
            // TODO should probably be in connectionToClient.DisconnectInternal
            //      because that's the NetworkServer's connection!
            NetworkServer.RemoveLocalConnection();
        }

        // true because local connections never timeout
        internal override bool IsAlive(float timeout) => true;
    }
}
