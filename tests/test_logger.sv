module top;

import hermes::Logger;
import hermes::LogEvent;
import hermes::hermes_add_dummy_serializer;

Logger logger;
LogEvent e;

initial begin
    // serialize everything
    hermes_add_dummy_serializer("*");
    logger = new("test");
    e = new();

    for (int i = 0; i < 2000; i++) begin
        e.reset();
        e.add_value_bool("bool", i % 2);
        e.add_value_uint8("uint8_1", i % 256);
        e.add_value_uint8("uint8_2", i % 256 + 1);
        e.add_value_uint16("uint16_1", i);
        e.add_value_uint16("uint16_2", i + 1);
        e.add_value_uint32("uint32_1", i);
        e.add_value_uint32("uint32_2", i + 1);
        e.add_value_uint64("uint64_1", i);
        e.add_value_uint64("uint64_2", i + 1);
        e.add_value_string("string_1", "aaa");
        e.add_value_string("string_2", "bbb");
        logger.log(e);
        #10;
    end
end

final begin
    Logger::final_();
end


endmodule