CREATE TABLE army.my_types (
    id                         bigserial NOT NULL PRIMARY KEY,
    my_smallint                smallint                     DEFAULT 0,
    my_integer                 integer                      DEFAULT 0,
    my_decimal                 decimal(14, 2)               DEFAULT 0.0,
    my_real                    real                         DEFAULT 0.0,
    my_double                  double precision             DEFAULT 0.0,
    my_boolean                 boolean                      DEFAULT FALSE,
    my_timestamp               timestamp                    DEFAULT NOW(),
    my_timestamp_4             timestamp(4)                 DEFAULT NOW(),
    my_zoned_timestamp         TIMESTAMP WITH TIME ZONE     DEFAULT NOW(),
    my_zoned_timestamp_6       TIMESTAMP(6) WITH TIME ZONE  DEFAULT NOW(),
    my_date                    date                         DEFAULT NOW(),
    my_time                    time                         DEFAULT NOW(),
    my_time_2                  time(2)                      DEFAULT NOW(),
    my_zoned_time              TIME WITH TIME ZONE          DEFAULT NOW(),
    my_zoned_time_1            TIME(1) WITH TIME ZONE       DEFAULT NOW(),
    my_bit                     bit(64)                      DEFAULT B'0000000000000000000000000000000000000000000000000000000000000000',
    my_interval                interval                     DEFAULT '00:00:00'::interval,
    my_varbit_64               BIT VARYING(64)              DEFAULT B'0',
    my_bytea                   bytea                        DEFAULT '\x00'::bytea,
    my_money                   money                        DEFAULT 0,
    my_varchar                 varchar(256)                 DEFAULT '',
    my_char                    char(256)                    DEFAULT '',
    my_text                    text                         DEFAULT '',
    my_json                    JSON                         DEFAULT '{}',
    my_jsonb                   jsonb                        DEFAULT '{}',
    my_xml                     XML,
    my_gender                  army.gender                  DEFAULT 'UNKNOWN',
    my_uuid                    uuid                         DEFAULT 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11',
    my_point                   point                        DEFAULT '(0,0)'::point,
    my_line                    line                         DEFAULT '{1,-1,0}'::line,
    my_line_segment            lseg                         DEFAULT '[ ( 0 , 0 ) , ( 1 , 1 ) ]',
    my_box                     box                          DEFAULT '( ( 0 , 0 ) , ( 1 , 1 ) )'::box,
    my_path                    PATH                         DEFAULT '[(0,0),(1,1)]'::PATH,
    my_polygon                 polygon                      DEFAULT '( ( 0 , 0 ) , ( 1 , 1 ) ,(2,2))'::polygon,
    my_circles                 circle                       DEFAULT '<(0,0),3>',
    my_cidr                    cidr                         DEFAULT '192.168.100.128/25'::cidr,
    my_inet                    inet                         DEFAULT '192.168.100.128/25'::inet,
    my_macaddr                 macaddr                      DEFAULT '08:00:2b:01:02:03'::macaddr,
    my_macaddr8                macaddr8                     DEFAULT '08:00:2b:01:02:03:04:05'::macaddr8,
    my_tsvector                tsvector                     DEFAULT 'a fat cat sat on a mat and ate a fat rat'::tsvector,
    my_tsquery                 tsquery                      DEFAULT 'fat & rat'::tsquery,
    my_int4_range              int4range                    DEFAULT 'empty'::int4range,
    my_smallint_array          smallint[]                   DEFAULT '{0}',
    my_smallint_array_2        smallint[][]                 DEFAULT '{{0}}',
    my_integer_array           integer[]                    DEFAULT '{0}',
    my_integer_array_2         integer[][]                  DEFAULT '{{0}}',
    my_bigint_array            bigint[]                     DEFAULT '{0}',
    my_bigint_array_2          bigint[][]                   DEFAULT '{{0}}',
    my_decimal_array           decimal[]                    DEFAULT '{0.0}',
    my_decimal_array_2         decimal[][]                  DEFAULT '{{0.0}}',
    my_real_array              real[]                       DEFAULT '{0.0}',
    my_real_array_2            real[][]                     DEFAULT '{{0.0}}',
    my_double_array            double precision[]           DEFAULT '{0.0}',
    my_double_array_2          double precision[][]         DEFAULT '{{0.0}}',
    my_boolean_array           bool[]                       DEFAULT '{FALSE}',
    my_boolean_array_2         bool[][]                     DEFAULT '{{FALSE}}',
    my_timestamp_array         timestamp[]                  DEFAULT '{''2021-08-20 11:49:23''}',
    my_timestamp_array_2       timestamp[][]                DEFAULT '{{''2021-08-20 11:49:23''}}',
    my_time_array              time[]                       DEFAULT '{11:49:23}',
    my_time_array_2            time[][]                     DEFAULT '{{''11:49:23''}}',
    my_zoned_timestamp_array   timestamp WITH TIME ZONE[]   DEFAULT '{''2021-08-20 11:49:23+08:00''}',
    my_zoned_timestamp_array_2 timestamp WITH TIME ZONE[][] DEFAULT '{{''2021-08-20 11:49:23+08:00''}}',
    my_zoned_time_array        time WITH TIME ZONE[]        DEFAULT '{''11:49:23+08:00''}',
    my_zoned_time_array_2      time WITH TIME ZONE[][]      DEFAULT '{{''11:49:23+08:00''}}',
    my_date_array              date[]                       DEFAULT '{''2021-08-20''}',
    my_date_array_2            date[][]                     DEFAULT '{{''2021-08-20''}}',
    my_bit_array               bit(64)[]                    DEFAULT '{B0000000000000000000000000000000000000000000000000000000000000000}',
    my_bit_array_2             bit(64)[][]                  DEFAULT '{{B0000000000000000000000000000000000000000000000000000000000000000}}',
    my_varbit_array            varbit(64)[]                 DEFAULT '{B0}',
    my_varbit_array_2          varbit(64)[][]               DEFAULT '{{B0}}',
    my_interval_array          interval[]                   DEFAULT '{PT0S}',
    my_interval_array_2        interval[][]                 DEFAULT '{{PT0S}}',
    my_bytea_array             bytea[]                      DEFAULT '{\x00}',
    my_bytea_array_2           bytea[][]                    DEFAULT '{{\x00}}',
    my_money_array             money[]                      DEFAULT '{0.0}',
    my_money_array_2           money[][]                    DEFAULT '{{0.0}}',
    my_varchar_array           varchar(225)[]               DEFAULT '{''''}',
    my_varchar_array_2         varchar[225][]               DEFAULT '{{''''}}',
    my_char_array              char(225)[]                  DEFAULT '{''''}',
    my_char_array_2            char(225)[][]                DEFAULT '{{''''}}',
    my_text_array              text[]                       DEFAULT '{''''}',
    my_text_array_2            text[][]                     DEFAULT '{{''''}}',
    my_json_array              json[]                       DEFAULT '{"{}"}',
    my_json_array_2            json[][]                     DEFAULT '{{"{}"}}',
    my_jsonb_array             jsonb[]                      DEFAULT '{"{}"}',
    my_jsonb_array_2           jsonb[][]                    DEFAULT '{{"{}"}}',
    my_xml_array               XML[],
    my_xml_array_2             XML[][],
    my_gender_array            army.gender[]                DEFAULT '{"UNKNOWN"}',
    my_gender_array_2          army.gender[][]              DEFAULT '{{"UNKNOWN"}}',
    my_uuid_array              uuid[]                       DEFAULT '{a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11}',
    my_uuid_array_2            uuid[][]                     DEFAULT '{{a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11}}',
    my_point_array             point[]                      DEFAULT '{"(0,0)"}',
    my_point_array_2           point[][]                    DEFAULT '{{"(0,0)"}}',
    my_line_array              line[]                       DEFAULT '{"{1,-1,0}"}',
    my_line_array_2            line[][]                     DEFAULT '{{"{1,-1,0}"}}',
    my_line_segment_array      lseg[]                       DEFAULT '{"[ ( 0 , 0 ) , ( 1 , 1 ) ]"}',
    my_line_segment_array_2    lseg[][]                     DEFAULT '{{"[ ( 0 , 0 ) , ( 1 , 1 ) ]"}}',
    my_box_array               box[]                        DEFAULT '{"( ( 0 , 0 ) , ( 1 , 1 ) )"}',
    my_box_array_2             box[][]                      DEFAULT '{{"( ( 0 , 0 ) , ( 1 , 1 ) )"}}',
    my_path_array              PATH[]                       DEFAULT '{"[(0,0),(1,1)]"}',
    my_path_array_2            PATH[][]                     DEFAULT '{{"[(0,0),(1,1)]"}}',
    my_polygon_array           polygon[]                    DEFAULT '{"( ( 0 , 0 ) , ( 1 , 1 ) ,(2,2))"}',
    my_polygon_array_2         polygon[][]                  DEFAULT '{{"( ( 0 , 0 ) , ( 1 , 1 ) ,(2,2))"}}',
    my_circles_array           circle[]                     DEFAULT '{"<(0,0),3>"}',
    my_circles_array_2         circle[][]                   DEFAULT '{{"<(0,0),3>"}}',
    my_cidr_array              cidr[]                       DEFAULT '{192.168.100.128/25}',
    my_cidr_array_2            cidr[][]                     DEFAULT '{{192.168.100.128/25}}',
    my_inet_array              inet[]                       DEFAULT '{192.168.100.128/25}',
    my_inet_array_2            inet[][]                     DEFAULT '{{192.168.100.128/25}}',
    my_macaddr_array           macaddr[]                    DEFAULT '{08:00:2b:01:02:03}',
    my_macaddr_array_2         macaddr[][]                  DEFAULT '{{08:00:2b:01:02:03}}',
    my_macaddr8_array          macaddr8[]                   DEFAULT '{08:00:2b:01:02:03:04:05}',
    my_macaddr8_array_2        macaddr8[][]                 DEFAULT '{{08:00:2b:01:02:03:04:05}}',
    my_tsvector_array          tsvector[]                   DEFAULT '{"a fat cat sat on a mat and ate a fat rat"}',
    my_tsvector_array_2        tsvector[][]                 DEFAULT '{{"a fat cat sat on a mat and ate a fat rat"}}',
    my_tsquery_array           tsquery[]                    DEFAULT '{"fat & rat"}',
    my_tsquery_array_2         tsquery[][]                  DEFAULT '{{"fat & rat"}}',
    my_int4_range_array        int4range[]                  DEFAULT '{empty}'

)