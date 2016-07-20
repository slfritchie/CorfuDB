package org.corfudb.cmdlets;

import org.corfudb.util.Utils;

import java.io.File;
import java.util.Arrays;

import static java.lang.System.exit;

/**
 * Routes symlink-files to the proper cmdlet.
 * Symlinks are in the bin directory.
 * They symlink to the script in scripts/cmdlet.sh
 * This script passes the name of the symlink as the first argument to this script.
 *
 * Created by mwei on 12/10/15.
 */
public class CmdletRouter {
    public static void main(String[] args) {
        String[] res = main2(args);
        for (int i = 1; i < res.length; i++) {
            System.out.println(res[0] + " " + res[i]);
        }
    }

    public static String[] main2(String[] args) {
        // We need to have at least the name of the cmdlet we are running.
        if (args.length < 1) {
            System.out.println("Please pass an available cmdlet.");
            return Utils.err("unavailable");
        }

        //Parse the cmdlet name. Sometimes it could be executed as ./<cmdlet>
        String cmdletName = args[0].substring(args[0].lastIndexOf(File.separatorChar) + 1);

        try {
            // Get the class for the cmdlet.
            Class<?> cmdlet = Class.forName("org.corfudb.cmdlets." + cmdletName);
            // Is it an ICmdlet?
            if (cmdlet.isAssignableFrom(ICmdlet.class)) {
                // No, abort.
                System.out.println("Cmdlet " + cmdletName + " is not a valid Corfu cmdlet!");
                return Utils.err("invalid");
            } else {
                try {
                    // Execute with the arguments other than the name of the cmdlet itself.
                    String[] res = ((ICmdlet) cmdlet.getConstructor().newInstance()).main(Arrays.copyOfRange(args, 1, args.length));
                    return res;
                } catch (Exception e) {
                    // Let the user know if an exception occurs.
                    System.out.println(e.getClass().getSimpleName() + " exception: " + e.getMessage());
                    return Utils.err(e.getClass().getSimpleName(), e.getMessage());
                }
            }
        } catch (ClassNotFoundException cnfe) {
            System.out.println("No cmdlet named " + cmdletName + " available!");
            return Utils.err("No such cmdlet", cmdletName);
        }
    }
}
