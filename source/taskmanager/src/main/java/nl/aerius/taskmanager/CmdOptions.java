/*
 * Copyright (c) Contributors to the project
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses/.
 */
package nl.aerius.taskmanager;

import java.io.File;
import java.io.FileNotFoundException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

/**
 * Class to process the command line options for the task manager.
 */
class CmdOptions {

  private static final String HELP = "help";
  private static final String VERSION = "version";
  private static final String CONFIG = "config";
  private static final String FILE_ARG_NAME = "file";

  private static final Option HELP_OPTION = new Option(HELP, "print this message");
  private static final Option VERSION_OPTION = new Option(VERSION, "print version information");
  private static final Option CONFIG_OPTION = new Option(CONFIG, true, "task manaegr options file");

  private final Options options;
  private final CommandLine cmd;

  public CmdOptions(final String[] args) throws ParseException {
    options = new Options();
    options.addOption(HELP_OPTION);
    options.addOption(VERSION_OPTION);
    CONFIG_OPTION.setArgName(FILE_ARG_NAME);
    CONFIG_OPTION.setRequired(true);
    options.addOption(CONFIG_OPTION);

    final CommandLineParser parser = new DefaultParser();
    cmd = parser.parse(options, args);
  }

  public String getConfigFile() throws FileNotFoundException {
    return getFileOption(CONFIG);
  }

  /**
   * Print help or version information if arguments specified. Returns true if the information was printed.
   * @return true if information was printed
   */
  public boolean printIfInfoOption() {
    boolean printed = true;
    if (cmd.hasOption(HELP)) {
      printHelp();
    } else if (cmd.hasOption(VERSION)) {
      printVersion();
    } else {
      printed = false;
    }
    return printed;
  }

  private void printHelp() {
    final HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp("aerius-taskmanager", options, true);
  }

  private void printVersion() {
    final String implementationVersion = this.getClass().getPackage().getImplementationVersion();
    System.out.println("Version: " + implementationVersion); // yes, a system.out thingy. It's meant for the user at startup.
  }

  /**
   * Get the file name argument as string and checks is the file exists.
   * @param option option to get file for
   * @return file name
   * @throws FileNotFoundException if file doesn't exist.
   */
  private String getFileOption(final String option) throws FileNotFoundException {
    final String sFile;
    if (cmd.hasOption(option)) {
      sFile = cmd.getOptionValue(option);
      final File file = new File(sFile);

      if (!file.exists() || !file.isFile()) {
        throw new FileNotFoundException(
            "Configuration file (" + sFile + " as supplied by -" + option + ") does not exist. ");
      }
    } else {
      sFile = null;
    }
    return sFile;
  }
}
