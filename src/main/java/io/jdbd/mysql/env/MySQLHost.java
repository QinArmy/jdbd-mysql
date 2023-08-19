package io.jdbd.mysql.env;

import io.jdbd.vendor.env.JdbdHost;

public interface MySQLHost extends JdbdHost.HostInfo {

    Protocol protocol();


}
