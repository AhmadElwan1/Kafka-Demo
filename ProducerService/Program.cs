using ProducerService.services;

var builder = WebApplication.CreateBuilder(args); // Initialize the web application builder

// Add services to the dependency injection container
builder.Services.AddControllers(); // Enable controllers for handling API requests
builder.Services.AddEndpointsApiExplorer(); // Enables OpenAPI/Swagger support
builder.Services.AddSingleton<KafkaProducerService>(); // Register Kafka producer as a singleton service

var app = builder.Build(); // Build the application pipeline

app.UseHttpsRedirection(); // Enforce HTTPS for security

// Map controller endpoints so the app can handle API requests
app.MapControllers();

app.Run(); // Run the application
