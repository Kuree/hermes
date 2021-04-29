import hermes::Tracker;
import hermes::Transaction;
import hermes::LogEvent;

class TrackerTest extends Tracker;
    // we only keep one of the curren transaction
    Transaction current_transaction;

    function new(string transaction_name);
        super.new(transaction_name);
    endfunction

    virtual function Transaction track(string topic, LogEvent event_);
        if (event_.time_ % 10 == 0) begin
            current_transaction = get_new_transaction();
        end
        if (event_.time_ % 10 == 9) begin
            current_transaction.finish();
        end

        // if we return the transaction, it means
        // the event belongs to that transaction
        return current_transaction;
    endfunction

endclass

module top;

import hermes::Logger;
import hermes::hermes_add_dummy_serializer;

Logger logger;
LogEvent e;
TrackerTest tracker;

initial begin
    // load the tracker
    hermes_add_dummy_serializer("*");
    tracker = new("test-transaction");
    Logger::add_tracker("test", tracker);
    logger = new("test");
    e = new();

    for (int i = 0; i < 2000; i++) begin
        e.reset();
        e.add_uint8("uint8_1", i % 256);
        logger.log(e);
        #1;
    end
end

final begin
    Logger::final_();
end


endmodule