package hdfs;

import java.util.Scanner;

public class Console {

    private static String usage = "Usage : java Console [node_root]";

    private static void showHelp() {
        System.out.print(
                "create [fname] [format] \t create a fname file into hdfs within format\n" +
                "cat [fname] \t\t\t\t show fname file content\n" +
                "delete [fname] \t\t\t\t delete fname file into hdfs\n"
        );
    }

    public static void main(String[] args) {
        Scanner input = new Scanner(System.in);
        String cmd;
        boolean exit = false;

        if (args.length < 1) {
            System.out.println(Console.usage);
            System.exit(1);
        }
        System.out.println("Hdfs console v0:");
        do {
            System.out.print("~$> ");
            cmd = input.next();
            switch (cmd) {
                case "e":
                case "exit":
                    exit = true;
                    break;
                case "h":
                case "help":
                    Console.showHelp();
                    break;
                default:
                    System.out.println("Unknown command: " + cmd + "\n" + "use h or help.");
                    break;
            }
        } while (!exit);
        System.out.println("bye!");
        System.exit(0);
    }
}
