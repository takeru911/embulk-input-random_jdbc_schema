package org.embulk.input.random_jdbc_schema;

import java.util.List;

import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.plugin.PluginType;
import org.embulk.spi.Column;
import org.embulk.spi.Exec;
import org.embulk.spi.ExecSession;
import org.embulk.spi.InputPlugin;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageOutput;
import org.embulk.spi.Schema;
import org.embulk.spi.SchemaConfig;
import org.embulk.spi.time.Timestamp;
import org.slf4j.Logger;

public class RandomJdbcSchemaInputPlugin
implements InputPlugin
{
	private final Logger logger = Exec.getLogger(RandomJdbcSchemaInputPlugin.class);
	public interface PluginTask
	extends Task
	{
		@Config("numOfRow")
		@ConfigDefault("1000")
		public int getNumOfRow();
	}

	@Override
	public ConfigDiff transaction(ConfigSource config,
			InputPlugin.Control control)
	{
		PluginTask task = config.loadConfig(PluginTask.class);
		Schema schemaFromJdbc;
		try {
			schemaFromJdbc = fetchSchemaByJDBC(config);
			for (int i = 0; i < schemaFromJdbc.getColumnCount(); i++) {
				Column column = schemaFromJdbc.getColumn(i);
				logger.info(String.format("column %d : name = %s, type = %s", i + 1, column.getName(), column.getType()));
			}
			config.set("columns", schemaFromJdbc.getColumns());
			int taskCount = 1;  // number of run() method calls

			return resume(task.dump(), schemaFromJdbc, taskCount, control);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	private Schema fetchSchemaByJDBC(ConfigSource config) throws Exception{
		ExecSession session = Exec.session();
		ConfigSource child = config.getNested("schema");
		String type = child.get(String.class, "type");
		InputPlugin input = session.newPlugin(InputPlugin.class, new PluginType(type));
		JdbcInputPluginHelper helper;
		helper = new JdbcInputPluginHelper(input);
		final Schema schemaFromJdbc = helper.getSchema(child);
		return schemaFromJdbc;
	}

	@Override
	public ConfigDiff resume(TaskSource taskSource,
			Schema schema, int taskCount,
			InputPlugin.Control control)
	{
		control.run(taskSource, schema, taskCount);
		return Exec.newConfigDiff();
	}

	@Override
	public void cleanup(TaskSource taskSource,
			Schema schema, int taskCount,
			List<TaskReport> successTaskReports)
	{
	}

	@Override
	public TaskReport run(TaskSource taskSource,
			Schema schema, int taskIndex,
			PageOutput output)
	{
		PluginTask task = taskSource.loadTask(PluginTask.class);
		int numOfRow = task.getNumOfRow();
		final PageBuilder pageBuilder = new PageBuilder(Exec.getBufferAllocator(), schema, output);

		for(int i = 0; i < numOfRow; i++){
			for(int j = 0; j < schema.getColumnCount(); j++){
				Column column = schema.getColumn(j);
				if(column.getType().getName().equals("long")){
					pageBuilder.setLong(schema.getColumn(j), i);
				}else if(column.getType().getName().equals("double")){
					pageBuilder.setDouble(schema.getColumn(j), i);
				}else if(column.getType().getName().equals("string")){
					pageBuilder.setString(schema.getColumn(j), "hoge");
				}else if(column.getType().getName().equals("timestamp")){
					pageBuilder.setTimestamp(schema.getColumn(j), Timestamp.ofEpochSecond(System.currentTimeMillis() / 1000));
				}else if(column.getType().getName().equals("boolean")){
					pageBuilder.setBoolean(schema.getColumn(j), false);
				}
			}
			pageBuilder.addRecord();            
		}	
		pageBuilder.finish();
		pageBuilder.close();
		return Exec.newTaskReport();
	}

	@Override
	public ConfigDiff guess(ConfigSource config)
	{
		return Exec.newConfigDiff();
	}
}
