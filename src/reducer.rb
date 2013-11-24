require 'eventmachine'
require 'pp'
require './ds_helper.rb'

module Reducer
  def receive_data(data) 
    dc = Ds::read(data)
    rdc = process(dc)

    rds = Ds::write(rdc)
    send_back("r:#{data}:#{rds}")  
  end

  def process(dc)
    grouping = dc.group_by{ |entry| entry[0] }
    grouping.map do |k,l|
      v = l.map{|x| x[1]}.inject{ |x, s| s+= x}
      [k, v]
    end
  end

  def send_back(data)
    puts data
    send_data(data)
    close_connection_after_writing
  end
end
