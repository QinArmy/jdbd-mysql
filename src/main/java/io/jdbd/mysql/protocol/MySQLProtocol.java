package io.jdbd.mysql.protocol;

import io.jdbd.vendor.protocol.DatabaseProtocol;

public interface MySQLProtocol extends DatabaseProtocol {


    long threadId();

}
