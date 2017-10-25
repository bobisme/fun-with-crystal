require "http/server"
require "json"

DEBUG = false

def debug(msg)
  STDERR.puts "DEBUG: #{msg}" if DEBUG
end

class Request
  JSON.mapping(
    words: String,
  )
end

alias WordChannel = Channel(NamedTuple(word: String, count: Int32))

class PartialReducer
  @state = Hash(String, Int32).new
  @ticks = Channel(Nil).new

  def initialize(
    @in_chan : WordChannel, @out_chan : WordChannel, @done : Channel(Nil))
  end

  private def handle_word(value)
    debug "got word: #{value}"
    word = value[:word]
    @state[word] = 0 unless @state[word]?
    @state[word] += value[:count]
  end

  def work
    spawn tick
    spawn do
      begin
        loop do
          break if @in_chan.closed? && @in_chan.empty?
          index, value = Channel.select(
            @in_chan.receive_select_action,
            @ticks.receive_select_action
          )
          case index
          when 0
            handle_word value.as(typeof(@in_chan.receive))
          when 1
            send_all
            @state.clear
          end
        end
        send_all
      ensure
        die
      end
    end
  end

  private def die
    debug "closing partial"
    @done.send nil
    @ticks.close
  end

  private def tick
    loop do
      return if @ticks.closed?
      @ticks.send nil
      sleep 0.5
    end
  end

  private def send_all
    debug "sending state"
    @state.each { |k, v| @out_chan.send({word: k, count: v}) }
  end
end

class Reducer
  @state = Hash(String, Int32).new

  def initialize(@in_chan : WordChannel, @out_chan : WordChannel); end

  def work
    spawn do
      while tup = @in_chan.receive?
        debug "got word #{tup}"
        word = tup[:word]
        @state[word] = 0 unless @state[word]?
        @state[word] += tup[:count]
      end
      send_all
    end
  end

  private def send_all
    debug "sending final"
    @state.each { |k, v| @out_chan.send({word: k, count: v}) }
    @out_chan.close
  end
end

class Mapper
  def initialize(@out_chan : WordChannel); end

  private def send_word(word)
    word = word.strip
    return if word.empty?
    debug "mapper: sent #{word}"
    @out_chan.send({word: word, count: 1})
  end

  def send(input : String)
    input.split.each { |word| send_word(word) }
  end

  def send(input : IO)
    while word = input.gets(" ", chomp: true)
      send_word word
    end
  end

  def close
    debug "close mapper"
    @out_chan.close
  end
end

class MapReduce
  getter :mapper
  @chan_1 = WordChannel.new(100)
  @chan_2 = WordChannel.new(100)
  @final = WordChannel.new(100)
  @mapper = Mapper.new(@chan_1)
  @partials_done = Channel(Nil).new(8)
  @partials = [] of PartialReducer

  def initialize
    @partials = (0...8).map do
      PartialReducer.new(@chan_1, @chan_2, @partials_done)
    end
    @reducer = Reducer.new(@chan_2, @final)
    @partials.each(&.work)
    @reducer.work
  end

  def get
    8.times { @partials_done.receive }
    @chan_2.close
    state = [] of NamedTuple(word: String, count: Int32)
    while value = @final.receive?
      state << value
    end
    state.sort_by! { |x| -x[:count] }
    state[0..(state.size / 5).to_i32]
  end
end

server = HTTP::Server.new(8080) do |ctx|
  ctx.response.content_type = "text/plain"
  begin
    body = ctx.request.body.not_nil!
  rescue
    next ctx.response.print "no body"
  end
  mr = MapReduce.new
  mr.mapper.send(body)
  mr.mapper.close
  ctx.response.print mr.get.to_json
end

puts "listening on 8080"
server.listen
