require 'eventmachine'
require 'pp'
require './ds_helper.rb'

module Mapper
  def receive_data(data)
    dc = Ds::read(data)
    mdc = process(dc)

    ids = Ds::write(mdc)
    send_back("m:#{data}:#{ids}")
  end

  def process(dc)
    dc.gsub!(/[\[\]\(\)\{\}\/\"\.\*,:;'+-=]/, ' ')    
    dc.split(' ').map { |word| [word, 1] }
  end

  def send_back(data)
    puts data
    send_data(data)
    close_connection_after_writing
  end
end
