import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Main {
  public static class ValueWithExpiry {
    String value;
    long expiry;

    ValueWithExpiry(String value, long expiry) {
      this.value = value;
      this.expiry = expiry;
    }

    boolean isExpired() {
      return expiry > 0 && System.currentTimeMillis() > expiry;
    }
  }

  private static Map<String, ValueWithExpiry> data = new ConcurrentHashMap<>();
  private static Map<String, List<String>> lists = new ConcurrentHashMap<>();

  public static void main(String[] args){
    System.out.println("Logs from your program will appear here!");

    ServerSocket serverSocket = null;
    int port = 6379;
    try {
      serverSocket = new ServerSocket(port);
      serverSocket.setReuseAddress(true);

      while (true) {
        Socket clientSocket = serverSocket.accept();
        Thread clientThread = new Thread(() -> clientHandler(clientSocket));
        clientThread.start();
      }

    } catch (IOException e) {
      System.out.println("IOException: " + e.getMessage());
    } finally {
      try {
        if (serverSocket != null) {
          serverSocket.close();
        }
      } catch (IOException e) {
        System.out.println("IOException: " + e.getMessage());
      }
    }
  }

  private static void clientHandler(Socket clientSocket) {
    try {
      OutputStream outputStream = clientSocket.getOutputStream();

      while (true) {
        byte[] buffer = new byte[1024];
        clientSocket.getInputStream().read(buffer);
        String input = new String(buffer);
        outputStream.write(handleCommand(input).getBytes());
      }
    } catch (IOException e) {
      System.out.println("IOException: " + e.getMessage());
    } finally {
      try {
        clientSocket.close();
      } catch (IOException e) {
        System.out.println("IOException: " + e.getMessage());
      }
    }
  }

  private static String handleCommand(String input) {
    System.out.println(input);
    String[] lines = input.trim().split("\r\n");

    String command, key, value;
    command = key = value = "";

    List<String> values = new ArrayList<>();

    int loopRun, start, stop;
    loopRun = start = stop = 0;
    long expiry = 0;
    
    for (int i = 0; i < lines.length; i++) {
      if (lines[i].startsWith("$") && i + 1 < lines.length) {
        if (command.isEmpty()) {
          command = lines[i + 1].toUpperCase();
        } else {
          loopRun++;
          if (command.equalsIgnoreCase("echo") && loopRun == 1) {
            key = lines[i + 1];
            break;
          } else if (command.equalsIgnoreCase("set")) {
            if (loopRun == 1) {
              key = lines[i + 1];
              System.out.println("key: " + key);
            } else if (loopRun == 2) {
              value = lines[i + 1];
              System.out.println("set: " + value);
            } else if (loopRun == 3) {
              if (i + 3 < lines.length && lines[i + 2].startsWith("$")) {
                expiry = Long.parseLong(lines[i + 3]);
                break;
              }
            }
          } else if (command.equalsIgnoreCase("get") && loopRun == 1) {
            key = lines[i+1];
            break;
          } else if (command.equalsIgnoreCase("rpush")) {
            if (loopRun == 1) {
              key = lines[i + 1];
            } else {
              values.add(lines[i + 1]);
            }
          } else if (command.equalsIgnoreCase("lrange")) {
            if (loopRun == 1) {
              key = lines[i + 1];
            } else if (loopRun == 2) {
              start = Integer.parseInt(lines[i + 1]);
            } else if (loopRun == 3) {
              stop = Integer.parseInt(lines[i + 1]);
              break;
            }
          } else if (command.equalsIgnoreCase("lpush")) {
            if (loopRun == 1) {
              key = lines[i + 1];
            } else {
              values.add(lines[i + 1]);
            }
          } else if (command.equalsIgnoreCase("llen")) {
            if (loopRun == 1) {
              key = lines[i + 1];
            }
          } else if (command.equalsIgnoreCase("lpop")) {
            if (loopRun == 1) {
              key = lines[i + 1];
            } else if (loopRun == 2) {
              start = Integer.parseInt(lines[i + 1]);
              break;
            }
          } else if (command.equalsIgnoreCase("blpop")) {
            if (loopRun == 1) {
              key = lines[i + 1];
            } else if (loopRun == lines.length - 1) {
              double timeoutSeconds = Double.parseDouble(lines[i + 1]);
              start = (int)(timeoutSeconds * 1000);
              break;
            } else {
              values.add(lines[i + 1]);
            }
          }
        }
      }
    }

    System.out.println("Command: " + command);

    switch (command) {
      case "PING":
        System.out.println("ping");
        return "+PONG\r\n";
      case "ECHO":
        return "$" + key.length() + "\r\n" + key + "\r\n";
      case "SET":
        System.out.println("set");
        long expiryTime = expiry > 0 ? System.currentTimeMillis() + expiry : 0;
        data.put(key, new ValueWithExpiry(value, expiryTime));
        return "+OK\r\n";
      case "GET":
        ValueWithExpiry storedValue = data.get(key);
        if (storedValue == null || storedValue.isExpired()) {
          if (storedValue != null && storedValue.isExpired()) {
            data.remove(key);
          }
          return "$-1\r\n";
        }
        return "$" + storedValue.value.length() + "\r\n" + storedValue.value + "\r\n";
      case "RPUSH":
        List<String> rpushList = lists.computeIfAbsent(key, k -> new ArrayList<>());
        rpushList.addAll(values);
        return ":" + rpushList.size() + "\r\n";
      case "LPUSH":
        List<String> lpushList = lists.computeIfAbsent(key, k -> new ArrayList<>());
        for (String val : values) {
          lpushList.add(0, val);
        }
        return ":" + lpushList.size() + "\r\n";
      case "LRANGE":
        List<String> lrangeList = lists.get(key);
        if (lrangeList == null || lrangeList.isEmpty()) {
          return "*0\r\n";
        }

        int size = lrangeList.size();
        if (start < 0) start = size + start;
        if (stop < 0) stop = size + stop;

        start = Math.max(0, start);
        stop = Math.min(size - 1, stop);

        if (start > stop || start >= size) {
          return "*0\r\n";
        }

        StringBuilder lrangeResponse = new StringBuilder();
        int rangeSize = stop - start + 1;
        lrangeResponse.append("*").append(rangeSize).append("\r\n");

        for (int i = start; i <= stop; i++) {
          String element = lrangeList.get(i);
          lrangeResponse.append("$").append(element.length()).append("\r\n");
          lrangeResponse.append(element).append("\r\n");
        }

        return lrangeResponse.toString();
      case "LLEN":
        List<String> llenList = lists.get(key);
        if (llenList == null) {
          return ":0\r\n";
        }
        return ":" + llenList.size() + "\r\n";
      case "LPOP":
        List<String> lpopList = lists.get(key);
        if (lpopList == null || lpopList.isEmpty()) {
          return "$-1\r\n";
        }

        int count = start > 0 ? start : 1;
        int actualCount = Math.min(count, lpopList.size());

        if (actualCount == 1) {
          String firstElement = lpopList.remove(0);
          return "$" + firstElement.length() + "\r\n" + firstElement + "\r\n";
        }

        StringBuilder lpopResponse = new StringBuilder();
        lpopResponse.append("*").append(actualCount).append("\r\n");

        for (int i = 0; i < actualCount; i++) {
          String element = lpopList.remove(0);
          lpopResponse.append("$").append(element.length()).append("\r\n");
          lpopResponse.append(element).append("\r\n");
        }

        return lpopResponse.toString();
      case "BLPOP":
        int timeout = start;
        long endTime = timeout == 0 ? Long.MAX_VALUE : timeout;
        
        List<String> keysToCheck = new ArrayList<>();
        keysToCheck.add(key);
        keysToCheck.addAll(values);
        
        while (System.currentTimeMillis() < endTime) {
          for (String checkKey : keysToCheck) {
            List<String> blpopList = lists.get(checkKey);
            if (blpopList != null && !blpopList.isEmpty()) {
              String element = blpopList.remove(0);
              return "*2\r\n$" + checkKey.length() + "\r\n" + checkKey + "\r\n$" + element.length() + "\r\n" + element + "\r\n";
            }
          }
          
          try {
            Thread.sleep(10);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return "$-1\r\n";
          }
        }
        
        return "$-1\r\n";
      default:
        System.out.println("default");
        return "+PONG\r\n";
    }
  }
}
