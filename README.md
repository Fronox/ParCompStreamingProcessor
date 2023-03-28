# Streaming Data Processor

This Scala project implements weather data processing from [Dark Sky API](https://darksky.net/dev) using Apache Spark streaming.

Data stream is simulated with [DataCollector](https://github.com/Fronox/DataCollector) module.
It periodically polls the API and saves data chuncs to a local FS, from where the processor fetches the raw data.

Presentation with description and details: [link](https://docs.google.com/presentation/d/10IWfhQ4k62SWA433ZcOyEN_WQfFCqIJwR7X5MsmXbGE/edit?usp=sharing)
