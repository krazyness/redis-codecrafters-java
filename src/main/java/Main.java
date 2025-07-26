import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
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

    int loopRun = 0;
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
        return "$" + storedValue.value.length() + "\r\n" + storedValue + "\r\n";
      default:
        System.out.println("default");
        return "+PONG\r\n";
    }
  }
}
