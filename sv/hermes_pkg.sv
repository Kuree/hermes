package hermes;

// DPI imports
// per LRM 35.5.1.3
// since we're not accessing any SV objects except the arguments but with some side-effects
// i.e. chaging the state of the logger, we don't need to declare DPI as context, but cannot
// use pure either
import "DPI-C" function void hermes_set_output_dir(input string directory);
import "DPI-C" function chandle hermes_create_logger(input string directory);
import "DPI-C" function void hermes_create_events(input chandle logger,
                                                  input longint unsigned times[]);
import "DPI-C" function void hermes_create_events_id(input chandle logger,
                                                     input longint unsigned times[],
                                                     output chandle event_handles[]);
import "DPI-C" function void hermes_set_values_bool(input chandle logger, input string names[],
                                                    input bit values[]);
import "DPI-C" function void hermes_set_values_uint8(input chandle logger, input string names[],
                                                     input byte unsigned values[]);
import "DPI-C" function void hermes_set_values_uint16(input chandle logger, input string names[],
                                                      input shortint unsigned values[]);
import "DPI-C" function void hermes_set_values_uint32(input chandle logger, input string names[],
                                                      input int unsigned values[]);
import "DPI-C" function void hermes_set_values_uint64(input chandle logger, input string names[],
                                                     input longint unsigned values[]);
import "DPI-C" function void hermes_set_values_string(input chandle logger, input string names[],
                                                      input string values[]);
import "DPI-C" function void hermes_send_events(input chandle logger);

import "DPI-C" function chandle hermes_create_tracker(input string name);
import "DPI-C" function chandle hermes_tracker_new_transaction(input chandle tracker);
import "DPI-C" function void hermes_transaction_finish(input chandle transaction);
import "DPI-C" function void hermes_retire_transaction(input chandle tracker,
                                                       input chandle transaction);
import "DPI-C" function void hermes_add_event_transaction(input chandle transaction,
                                                          input chandle event_);

import "DPI-C" function void hermes_final();
// help functions
import "DPI-C" function void hermes_add_dummy_serializer(input string topic);
import "DPI-C" function void hermes_set_serializer_dir(input string path);


// class wrapper
class LogEvent;
    // attributes
    longint unsigned time_;
    // value holders
    bit               bool[string];
    byte unsigned     uint8[string];
    shortint unsigned uint16[string];
    int unsigned      uint32[string];
    longint unsigned  uint64[string];
    string            string_[string];
    
    function new();
        reset();
    endfunction

    function void reset();
        this.time_ = $time();
    endfunction

    // have shortened names as well
    function void add_bool(string name, bit value);
        bool[name] = value;
    endfunction

    function void add_uint8(string name, byte unsigned value);
        uint8[name] = value;
    endfunction

    function void add_uint16(string name, shortint unsigned value);
        uint16[name] = value;
    endfunction

    function void add_uint32(string name, int unsigned value);
        uint32[name] = value;
    endfunction

    function void add_uint64(string name, longint unsigned value);
        uint64[name] = value;
    endfunction

    function void add_string(string name, string value);
        string_[name] = value;
    endfunction

    function static LogEvent copy();
        automatic LogEvent e = new();
        e.time_ = time_;
        // per 7.9.9, associated array assignment is a copy assignment
        e.bool = bool;
        e.uint8 = uint8;
        e.uint16 = uint16;
        e.uint32 = uint32;
        e.uint64 = uint64;
        e.string_ = string_;
        return e;
    endfunction

endclass

typedef class Tracker;
class Logger;
    // local values
    local string            event_name;
    local bit               bool[$];
    local string            bool_names[$];
    local byte unsigned     uint8[$];
    local string            uint8_names[$];
    local shortint unsigned uint16[$];
    local string            uint16_names[$];
    local int unsigned      uint32[$];
    local string            uint32_names[$];
    local longint unsigned  uint64[$];
    local string            uint64_names[$];
    local string            string_[$];
    local string            string_names[$];
    local longint unsigned  times[$];
    // keep track of number of events
    local int               num_events;
    // the actual logger
    local chandle           logger_;
    // flush threshold
    local static int        num_events_batch = 256;
    // all the loggers are here
    static Logger loggers[$];
    // trackers as well
    // notice that we can't implement sub/pub since we can pass systemverilog
    // functions around
    static local Tracker trackers[string][$];
    // unused funless trackers are used
    local LogEvent       tracker_events[$];
    // whether to record where the logging happens. if true, raw location will
    // be stored in the event. default is false
    bit                  store_location = 0;
    local  bit           has_event_;

    function new(string event_name);
        this.logger_ = hermes_create_logger(event_name);
        loggers.push_back(this);
        this.num_events = 0;
        this.event_name = event_name;
        this.has_event_ = 1'b0;
    endfunction

    function void log(LogEvent event_);
        // add it to the cached value
        times.push_back(event_.time_);

        if (store_location) begin
            event_.string_["location"] = $sformatf("%m");
        end

        if (event_.bool.size() > 0) begin
            foreach(event_.bool[name]) begin
                bool.push_back(event_.bool[name]);
                if (!has_event_) begin
                    bool_names.push_back(name);
                end
            end
            if (has_event_ && event_.bool.size() != bool_names.size()) begin
                $display("[ERROR]: unmatched log item names. Expected: %0d got %0d", bool_names.size(), event_.bool.size());
            end
        end

        if (event_.uint8.size() > 0) begin
            foreach(event_.uint8[name]) begin
                uint8.push_back(event_.uint8[name]);
                if (!has_event_) begin
                    uint8_names.push_back(name);
                end
            end
            if (has_event_ && event_.uint8.size() != uint8_names.size()) begin
                $display("[ERROR]: unmatched log item names. Expected: %0d got %0d", uint8_names.size(), event_.uint8.size());
            end
        end

        if (event_.uint16.size() > 0) begin
            foreach(event_.uint16[name]) begin
                uint16.push_back(event_.uint16[name]);
                if (!has_event_) begin
                    uint16_names.push_back(name);
                end
            end
            if (has_event_ && event_.uint16.size() != uint16_names.size()) begin
                $display("[ERROR]: unmatched log item names. Expected: %0d got %0d", uint16_names.size(), event_.uint16.size());
            end
        end

        if (event_.uint32.size() > 0) begin
            foreach(event_.uint32[name]) begin
                uint32.push_back(event_.uint32[name]);
                if (!has_event_) begin
                    uint32_names.push_back(name);
                end
            end
            if (has_event_ && event_.uint32.size() != uint32_names.size()) begin
                $display("[ERROR]: unmatched log item names. Expected: %0d got %0d", uint32_names.size(), event_.uint32.size());
            end
        end

        if (event_.uint64.size() > 0) begin
            foreach(event_.uint64[name]) begin
                uint64.push_back(event_.uint64[name]);
                if (!has_event_) begin
                    uint64_names.push_back(name);
                end
            end
            if (has_event_ && event_.uint64.size() != uint64_names.size()) begin
                $display("[ERROR]: unmatched log item names. Expected: %0d got %0d", uint64_names.size(), event_.uint64.size());
            end
        end

        if (event_.string_.size() > 0) begin
            foreach(event_.string_[name]) begin
                string_.push_back(event_.string_[name]);
                if (!has_event_) begin
                    string_names.push_back(name);
                end
            end
            if (has_event_ && event_.string_.size() != string_names.size()) begin
                $display("[ERROR]: unmatched log item names. Expected: %0d got %0d", string_names.size(), event_.string_.size());
            end
        end

        if (trackers.size() > 0) begin
            tracker_events.push_back(event_.copy());
        end

        this.num_events++;
        if (this.num_events >= this.num_events_batch) begin
            this.flush();
        end

        has_event_ = 1'b1;
    endfunction

    local function automatic void flush();
        // we made assumption that the logger only takes one type of events
        // batches
        longint unsigned  times_batch[];
        bit               bool_batch[];
        byte unsigned     uint8_batch[];
        shortint unsigned uint16_batch[];
        int unsigned      uint32_batch[];
        longint unsigned  uint64_batch[];
        string            string_batch[];
        string            bool_name_batch[];
        string            uint8_name_batch[];
        string            uint16_name_batch[];
        string            uint32_name_batch[];
        string            uint64_name_batch[];
        string            string_name_batch[];
        // event ids. we only use this if there is tracker attached
        chandle           event_handles[];

        if (num_events == 0) begin
            // no events
            return;
        end

        times_batch = new[num_events];
        foreach (times[i]) begin
            times_batch[i] = times[i];
        end

        if (trackers.size() > 0) begin
            event_handles = new[num_events];
        end

        // maybe the simulator will do zero-copy aliasing?
        bool_batch = bool;
        bool_name_batch = bool_names;
        uint8_batch = uint8;
        uint8_name_batch = uint8_names;
        uint16_batch = uint16;
        uint16_name_batch = uint16_names;
        uint32_batch = uint32;
        uint32_name_batch = uint32_names;
        uint64_batch = uint64;
        uint64_name_batch = uint64_names;
        string_batch = string_;
        string_name_batch = string_names;

        // call DPI functions to store data
        // create events
        if (trackers.size() == 0) begin
            hermes_create_events(logger_, times_batch);
        end else begin
            hermes_create_events_id(logger_, times_batch, event_handles);
        end

        if (bool_batch.size() > 0) begin
            hermes_set_values_bool(logger_, bool_name_batch, bool_batch);
        end
        if (uint8_batch.size() > 0) begin
            hermes_set_values_uint8(logger_, uint8_name_batch, uint8_batch);
        end
        if (uint16_batch.size() > 0) begin
            hermes_set_values_uint16(logger_, uint16_name_batch, uint16_batch);
        end
        if (uint32_batch.size() > 0) begin
            hermes_set_values_uint32(logger_, uint32_name_batch, uint32_batch);
        end
        if (uint64_batch.size() > 0) begin
            hermes_set_values_uint64(logger_, uint64_name_batch, uint64_batch);
        end
        if (string_batch.size() > 0) begin
            hermes_set_values_string(logger_, string_name_batch, string_batch);
        end

        // track it if necessary
        if (trackers.size() > 0) begin
            foreach(trackers[name]) begin
                if (name == this.event_name) begin
                    foreach(trackers[name][i]) begin
                        trackers[name][i].trigger(name, tracker_events, event_handles);
                    end
                end
            end
            tracker_events.delete();
        end

        // send events
        hermes_send_events(logger_);

        // clear up
        num_events = 0;
        times.delete();
        bool.delete();
        uint8.delete();
        uint16.delete();
        uint32.delete();
        uint64.delete();
        string_.delete();
    endfunction

    static function void final_();
        foreach(loggers[i]) begin
            loggers[i].flush();
        end
        hermes_final();
    endfunction

    static function void add_tracker(string topic, Tracker tracker);
        trackers[topic].push_back(tracker);
    endfunction

    static function void set_num_event_batch(int num);
        num_events_batch = num;
    endfunction

    static function void flush_all();
        foreach (loggers[i]) begin
            loggers[i].flush();
        end
    endfunction

endclass


class Transaction;
    chandle transaction_handle;
    bit finished;

    function new(chandle t);
        transaction_handle = t;
    endfunction

    function void add_event(chandle event_);
        hermes_add_event_transaction(transaction_handle, event_);
    endfunction

    function void finish();
        finished = 1'b1;
    endfunction

endclass

virtual class Tracker;
    local chandle tracker_;

    function new(string transaction_name);
        tracker_ = hermes_create_tracker(transaction_name);
    endfunction

    pure virtual function Transaction track(string topic, LogEvent event_);

    function void trigger(string name, LogEvent events[$], chandle event_handles[]);
        $display("event size: %d handle size: %d", events.size(), event_handles.size());
        foreach(events[i]) begin
            Transaction transaction;
            transaction = track(name, events[i]);

            if (transaction == null) begin
                return;
            end

            transaction.add_event(event_handles[i]);

            if (transaction.finished) begin
                hermes_retire_transaction(tracker_, transaction.transaction_handle);
            end
        end
    endfunction

    function Transaction get_new_transaction();
        automatic chandle t;
        automatic Transaction transaction;
        t = hermes_tracker_new_transaction(tracker_);
        transaction = new(t);
        return transaction;
    endfunction

endclass

endpackage