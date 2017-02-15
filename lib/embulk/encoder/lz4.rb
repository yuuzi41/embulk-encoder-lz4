Embulk::JavaPlugin.register_encoder(
  "lz4", "org.embulk.encoder.lz4.Lz4EncoderPlugin",
  File.expand_path('../../../../classpath', __FILE__))
