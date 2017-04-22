Embulk::JavaPlugin.register_input(
  "random_jdbc_schema", "org.embulk.input.random_jdbc_schema.RandomJdbcSchemaInputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
