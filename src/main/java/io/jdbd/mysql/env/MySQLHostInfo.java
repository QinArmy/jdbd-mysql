package io.jdbd.mysql.env;

import io.jdbd.vendor.env.JdbdHost;

public interface MySQLHostInfo extends JdbdHost.HostInfo {

    Protocol protocol();


}
