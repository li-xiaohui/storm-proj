/**
 * 
 */
package mygroup;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

/**
 * @author xiaohui
 *
 */
public class DBWriterBolt extends BaseRichBolt {

	static Connection connection = null;
	String tableName = "region_car_sample";
	PreparedStatement prepStatement = null;
	private OutputCollector _collector;

	/*
	 * (non-Javadoc)
	 * 
	 * @see backtype.storm.task.IBolt#prepare(java.util.Map,
	 * backtype.storm.task.TopologyContext, backtype.storm.task.OutputCollector)
	 */
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		_collector = collector;
		try {
			Class.forName("com.mysql.jdbc.Driver");
		} catch (ClassNotFoundException e) {
			System.out.println("Driver not found");
			e.printStackTrace();
		}

		try {
			connection = DriverManager
					.getConnection("jdbc:mysql://localhost/test?"
							+ "user=root&password=root");

		} catch (SQLException e) {
			e.printStackTrace();
		}

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see backtype.storm.task.IBolt#execute(backtype.storm.tuple.Tuple)
	 */
	@Override
	public void execute(Tuple input) {

		String regionName = input.getString(0);
		Integer count = input.getInteger(1);
		try {
			// Insert Query
			String sql = "INSERT INTO " + tableName
					+ "(region_name, num_cars) VALUES (?, ?) ";

			prepStatement = connection.prepareStatement(sql);

			if (prepStatement == null) {
				System.out.println("prepStatment is null!");

			} else {
				prepStatement.setString(1, regionName);
				prepStatement.setInt(2, count);
				prepStatement.execute();
				
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if (prepStatement != null) {
				try {
					prepStatement.close();

				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * backtype.storm.topology.IComponent#declareOutputFields(backtype.storm
	 * .topology.OutputFieldsDeclarer)
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub

	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		super.cleanup();

		if (connection != null) {
			try {
				connection.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

}
