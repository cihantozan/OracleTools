package oracletools.util;

public class OracleConnection {
	private String user;
	private String password;
	private String host;
	private int port;
	private String databaseName;
	
	
	
	public OracleConnection() {		
	}

	public OracleConnection(String user, String password, String host, int port, String databaseName) {
		super();
		this.user = user;
		this.password = password;
		this.host = host;
		this.port = port;
		this.databaseName = databaseName;
	}
	
	
		
	public String getConnectionString() {
		return "jdbc:oracle:thin:@"+this.host+":"+this.port+"/"+this.databaseName;
		//return "jdbc:oracle:thin:@"+this.host;
	}

	public String getUser() {
		return user;
	}
	public void setUser(String user) {
		this.user = user;
	}
	public String getPassword() {
		return password;
	}
	public void setPassword(String password) {
		this.password = password;
	}
	public String getHost() {
		return host;
	}
	public void setHost(String host) {
		this.host = host;
	}
	public int getPort() {
		return port;
	}
	public void setPort(int port) {
		this.port = port;
	}
	public String getDatabaseName() {
		return databaseName;
	}
	public void setDatabaseName(String databaseName) {
		this.databaseName = databaseName;
	}
	
	

	
}
