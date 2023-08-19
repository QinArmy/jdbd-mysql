package io.jdbd.mysql.protocol.client;

 interface MetaAdjutant {

     TaskAdjutant adjutant();

     void updateSequenceId(int sequenceId);
 }
