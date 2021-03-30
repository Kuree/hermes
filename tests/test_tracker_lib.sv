import "DPI-C" function void test_tracker_lib();

module top;

import hermes::Logger;
import hermes::LogEvent;
import hermes::hermes_add_dummy_serializer;

Logger logger;
LogEvent e;

initial begin
    // load the tracker
    test_tracker_lib();
    logger = new("test");
    e = new();

    for (int i = 0; i < 2000; i++) begin
        e.reset();
        e.add_value_uint8("uint8_1", i % 256);
        logger.log(e);
        #10;
    end
end

final begin
    Logger::final_();
end


endmodule