module Ds
  class << self 
    def write(object)
      dsname = ufname
      File.open(dsname, 'w') do |f|
        Marshal.dump(object, f) 
      end
      dsname
    end
    def read(dsname)
      object = if File.exists?(dsname)
                 File.open(dsname) do|file|
                   Marshal.load(file)
                 end
               else
                 0
               end

    end
    def merge(dsnames)
      dc1 = read(dsnames[0])
      dc2 = read(dsnames[1])
      write(dc1+dc2)
    end
    def get_peer
      port, ip = Socket.unpack_sockaddr_in(get_peername)
    end
    private 
    def ufname
      "ds-#{Time.now.strftime("%d%m%Y")}-#{Random.rand}.ds"
    end
  end
end

