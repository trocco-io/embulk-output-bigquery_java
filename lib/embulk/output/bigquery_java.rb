Embulk::JavaPlugin.register_output(
  "bigquery_java", "org.embulk.output.bigquery_java.BigqueryJavaOutputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
