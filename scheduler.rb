require 'eventmachine'
require './ds_helper.rb'

MNODES = [
  ['localhost', 5550],
  ['localhost', 5551],
  ['localhost', 5552]
]

RNODES = [
  ['localhost', 4440],
  ['localhost', 4441],
  ['localhost', 4442]
]

class Scheduler < EM::Connection
  @@ds = []
  @@ids = []
  @@rds = []
  @@jobs = 0

  def initialize(data)
    @ds ||= data
  end

  def post_init
    send_data(@ds)
  end

  def receive_data(data)
    Scheduler.collect(data)
  end

  def unbind
    @@jobs -= 1
    Scheduler.stop_if_finished
    Scheduler.send_to_workers    
  end

  def self.collect(data)
    p data
    rep = data.split(':')
    case
    when rep[0] == 'm'
      @@ids << rep[2]
    when rep[0] == 'r'
      @@rds << rep[2]
    end
  end

  def self.send_to_workers
    Scheduler.send_map_job if @@ds.size > 0
    Scheduler.send_reduce_job if @@ids.size > 0
    Scheduler.merge if @@rds.size > 1
  end

  def self.send_map_job
    p 'map'
    info
    MNODES.zip(@@ds).each do |node, ds|
      next if ds.nil?
      @@ds -= [ ds ]
      @@jobs += 1
      EM.connect(node[0], node[1], Scheduler, ds) 
    end
    info
  end

  def self.send_reduce_job
    p 'reduce'
    info
    RNODES.zip(@@ids).each do |node, ds|
      next if ds.nil?
      @@jobs += 1
      @@ids -= [ ds ]
      EM.connect(node[0], node[1], Scheduler, ds) 
    end
    info
  end

  def self.merge
    info
    p 'merge'
    @@rds.each_slice(2) do |ds|
      @@rds -= ds
      @@ids << if ds.size < 2
              ds.first
            else
              Ds::merge(ds)
            end
    end
    info
    Scheduler.send_to_workers
  end

  def self.ds(*ts)
    ts.each do |ts|
      @@ds << Ds::write(File.read(ts))
    end
  end

  def self.stop_if_finished
    if @@jobs == 0 and @@ids.empty? and @@rds.size == 1
      puts "== convergence =="
      p @@rds
      File.open('result.txt', 'w') do |f|
         f.write(Ds::read(@@rds.first).sort)
      end
      EM.stop
    end
  end

  def self.info
    puts "++"
    p @@jobs
    p @@ds
    p @@ids
    p @@rds
    puts "--"
  end

  def self.start
    send_to_workers 
  end
end

if __FILE__ == $0
  EM.run do
    Scheduler.ds 'a.en.txt', 'b.en.txt', 'c.en.txt', 'd.en.txt', 'e.en.txt'
    Scheduler.start
  end
end
