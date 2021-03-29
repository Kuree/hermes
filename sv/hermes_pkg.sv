package hermes;

// DPI imports
import "DPI-C" function void hermes_set_output_dir(string directory);
import "DPI-C" function chandle hermes_create_logger(string directory);
import "DPI-C" function void hermes_create_events(chandle logger,
                                                  longint unsigned times[]);
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
import "DPI-C" function void hermes_send_events(chandle logger);
import "DPI-C" function void hermes_final();


// class wrapper
class LogEvent;
    // attributes
    time time_;
    // value holders
    byte unsigned     uint8[string];
    shortint unsigned uint16[string];
    int unsigned      uint32[string];
    longint unsigned  uint64[string];
    string            string_[string];
    
    function new();
        this.time_ = $time();
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
        int uint8_size = uint8.size() / num_events;
        int uint16_size = uint16.size() / num_events;
        int uint32_size = uint32.size() / num_events;
        int uint64_size = uint64.size() / num_events;
        int string_size = string_.size() / num_events;

        int uint8_count = 0, uint16_count = 0, uint32_count = 0, uint64_count = 0,
            string_count = 0;



        // we specify each chunk
        for (int i = 0; i < num_events; i++) begin;
            for (int j = 0; i < uint8_size; j++) begin
                
            end
        end

    endfunction

    static function final_();
        foreach(loggers[i]) begin
            loggers[i].flush();
        end
    endfunction

endclass

endpackage