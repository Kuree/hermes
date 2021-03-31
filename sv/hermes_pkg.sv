package hermes;

// DPI imports
import "DPI-C" function void hermes_set_output_dir(string directory);
import "DPI-C" function chandle hermes_create_logger(string directory);
import "DPI-C" function void hermes_create_events(input chandle logger,
                                                  longint unsigned times[]);
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
import "DPI-C" function void hermes_final();
// help functions
import "DPI-C" function void hermes_add_dummy_serializer(input string topic);


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

    function void add_value_bool(string name, bit value);
        bool[name] = value;
    endfunction

    function void add_value_uint8(string name, byte unsigned value);
        uint8[name] = value;
    endfunction

    function void add_value_uint16(string name, shortint unsigned value);
        uint16[name] = value;
    endfunction

    function void add_value_uint32(string name, int unsigned value);
        uint32[name] = value;
    endfunction

    function void add_value_uint64(string name, longint unsigned value);
        uint64[name] = value;
    endfunction

    function void add_value_string(string name, string value);
        string_[name] = value;
    endfunction
endclass


class Logger;
    // local values
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
    local static int        num_events_batch = 1024;
    // all the loggers are here
    static Logger loggers[$];

    function new(string event_name);
        this.logger_ = hermes_create_logger(event_name);
        loggers.push_back(this);
        this.num_events = 0;
    endfunction

    function void log(LogEvent event_);
        // add it to the cached value
        times.push_back(event_.time_);

        if (event_.bool.size() > 0) begin
             foreach(event_.bool[name]) begin
                bool.push_back(event_.bool[name]);
                bool_names.push_back(name);
            end
        end

        if (event_.uint8.size() > 0) begin
            foreach(event_.uint8[name]) begin
                uint8.push_back(event_.uint8[name]);
                uint8_names.push_back(name);
            end
        end

        if (event_.uint16.size() > 0) begin
            foreach(event_.uint16[name]) begin
                uint16.push_back(event_.uint16[name]);
                uint16_names.push_back(name);
            end
        end

        if (event_.uint32.size() > 0) begin
            foreach(event_.uint32[name]) begin
                uint32.push_back(event_.uint32[name]);
                uint32_names.push_back(name);
            end
        end

        if (event_.uint64.size() > 0) begin
            foreach(event_.uint64[name]) begin
                uint64.push_back(event_.uint64[name]);
                uint64_names.push_back(name);
            end
        end

        if (event_.string_.size() > 0) begin
            foreach(event_.string_[name]) begin
                string_.push_back(event_.string_[name]);
                string_names.push_back(name);
            end
        end

        this.num_events++;
        if (this.num_events > this.num_events_batch) begin
            this.flush();
        end
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

        times_batch = new[num_events];
        foreach (times[i]) begin
            times_batch[i] = times[i];
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
        hermes_create_events(logger_, times_batch);
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

        // send events
        hermes_send_events(logger_);

        // clear up
        num_events = 0;
        times.delete();
        bool.delete();
        bool_names.delete();
        uint8.delete();
        uint8_names.delete();
        uint16.delete();
        uint16_names.delete();
        uint32.delete();
        uint32_names.delete();
        uint64.delete();
        uint64_names.delete();
        string_.delete();
        string_names.delete();
    endfunction

    static function void final_();
        foreach(loggers[i]) begin
            loggers[i].flush();
        end
        hermes_final();
    endfunction

endclass

endpackage