require 'rubygems'
require 'daemons'
require 'eventmachine'
require './mapper'
require './reducer'

EM.run do
  puts 'mappers'
  [5550, 5551, 5552].each do |port|
    EM.start_server('localhost', port, Mapper)
    puts " #{port}"
  end

  puts 'reducers'
  [4440, 4441, 4442].each do |port|
    EM.start_server('localhost', port, Reducer)
    puts " #{port}"
  end

  puts 'started!'
end

=begin
task1=Daemons.call(:multiple => true) do
  puts "hello, needed to create multiple..."
end

[5550, 5551, 5552].each do |port|
  Daemons.call do
    Em.run do
      Em.start_server('localhost', port, Mapper)
    end
  end
end

[4440, 4441, 4442].each do |port|
  Daemons.call do
    Em.run do
      Em.start_server('localhost', port, Reducer)
    end
  end
end
=end
