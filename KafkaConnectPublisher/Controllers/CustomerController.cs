using Confluent.Kafka;
using KafkaConnectPublisher.Dto;
using Microsoft.AspNetCore.Mvc;
using System;
using System.Text.Json;
using System.Threading.Tasks;

namespace KafkaConnectPublisher.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class CustomerController : ControllerBase
    {
        [HttpPost]
        public async Task<IActionResult> SendObject(CustomerDto customerDto)
        {
            var config = new ProducerConfig { BootstrapServers = "localhost:9092" };

            using (var producer = new ProducerBuilder<Null, string>(config).Build())
            {
                try
                {
                    var dtoJson = JsonSerializer.Serialize(customerDto);
                    var response = await producer.ProduceAsync("customer-queue", new Message<Null, string> { Value = dtoJson });
                    return Ok($"Delivered '{response.Value}' to '{response.TopicPartitionOffset}'");
                }
                catch (ProduceException<Null, string> e)
                {
                    return BadRequest(e.Error.Reason);
                }
            }
        }

        [HttpGet("{message}")]
        public async Task<IActionResult> SendString(string message)
        {
            var config = new ProducerConfig { BootstrapServers = "localhost:9092" };

            using (var p = new ProducerBuilder<Null, string>(config).Build())
            {
                try
                {
                    var response = await p.ProduceAsync("client-create", new Message<Null, string> { Value = "Mensagem para envio" });
                    return Ok($"Delivered '{response.Value}' to '{response.TopicPartitionOffset}'");
                }
                catch (ProduceException<Null, string> e)
                {
                    return BadRequest(e.Error.Reason);
                }
            }
        }
    }
}
