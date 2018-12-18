using System;
using System.Threading;
using System.Threading.Tasks;
using NServiceBus;
using NServiceBus.Pipeline;
using NServiceBus.Transport.AzureServiceBus;

namespace OversizedSends
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var configuration = new EndpointConfiguration("Samples.OversizedSender");
            configuration.UsePersistence<InMemoryPersistence>();
            configuration.UseSerialization<NewtonsoftSerializer>();
            configuration.EnableInstallers();

            configuration.Pipeline.Register(new AdjustmentBehaviorForSendsOutsideTheMessageHandlingPipeline(), "Trims messages");
            configuration.Pipeline.Register(new AdjustmentIndicationBehavior(), "Adjustment indication behavior");
            configuration.Pipeline.Register(new AdjustmentBehaviorForSendsThatAreTriggeredAsPartOfAnIncomingMessage(), "Adjustment behavior");

            var transport = configuration.UseTransport<AzureServiceBusTransport>();
            transport.UseForwardingTopology();
            transport.ConnectionString(Environment.GetEnvironmentVariable("AzureServiceBus_ConnectionString"));

            IEndpointInstance endpoint = null;
            try
            {
                endpoint = await Endpoint.Start(configuration);
                // Fails
                await endpoint.SendLocal(new MessageThatKicksOff());
                // Works
                await endpoint.SendLocal(new MyMessageThatSometimesFails { Property = new string('a', 256 * 1024) });
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);    
            }

            Console.ReadLine();

            if (endpoint != null)
            {
                await endpoint.Stop();
            }
        }
    }

    public class MyKickOffHandler : IHandleMessages<MessageThatKicksOff>
    {
        public async Task Handle(MessageThatKicksOff message, IMessageHandlerContext context)
        {
            Console.WriteLine(nameof(MyKickOffHandler));
            await context.SendLocal(new MyMessageThatSometimesFails { Property = new string('a', 256 * 1024) });
            await context.SendLocal(new MyMessageThatSometimesFails { Property = new string('b', 256 * 1024) });
        }
    }

    public class MyHandler : IHandleMessages<MyMessageThatSometimesFails>
    {
        public Task Handle(MyMessageThatSometimesFails message, IMessageHandlerContext context)
        {
            Console.WriteLine(nameof(MyHandler));
            Console.WriteLine(message.Property);
            return Task.CompletedTask;
        }
    }

    public class MessageThatKicksOff : ICommand { }

    public class MyMessageThatSometimesFails : ICommand
    {
        public string Property { get; set; }
    }

    public class AdjustmentIndicationBehavior : Behavior<ITransportReceiveContext>
    {
        public static AsyncLocal<bool> NeedsAdjustments = new AsyncLocal<bool>();

        public override async Task Invoke(ITransportReceiveContext context, Func<Task> next)
        {
            try
            {
                await next().ConfigureAwait(false);
            }
            catch (MessageTooLargeException)
            {
                NeedsAdjustments.Value = true;
                await next().ConfigureAwait(false);
            }
        }
    }

    public class AdjustmentBehaviorForSendsThatAreTriggeredAsPartOfAnIncomingMessage : Behavior<IOutgoingLogicalMessageContext>
    {
        public override async Task Invoke(IOutgoingLogicalMessageContext context, Func<Task> next)
        {
            if (AdjustmentIndicationBehavior.NeedsAdjustments.Value && context.Message.Instance is MyMessageThatSometimesFails message)
            {
                Console.WriteLine("Trimming oversized message...");
                context.UpdateMessage(new MyMessageThatSometimesFails { Property = message.Property.Substring(0, 250) });
            }

            await next().ConfigureAwait(false);
        }
    }

    public class AdjustmentBehaviorForSendsOutsideTheMessageHandlingPipeline : Behavior<IOutgoingLogicalMessageContext>
    {
        public override async Task Invoke(IOutgoingLogicalMessageContext context, Func<Task> next)
        {
            try
            {
                await next().ConfigureAwait(false);
            }
            catch (MessageTooLargeException)
            {
                if (context.Message.Instance is MyMessageThatSometimesFails message)
                {
                    Console.WriteLine("Trimming oversized message...");
                    context.UpdateMessage(new MyMessageThatSometimesFails { Property = message.Property.Substring(0, 250)});

                    await next().ConfigureAwait(false);
                    return;
                }

                throw;
            }
        }
    }
}
