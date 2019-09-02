/*
 * Copyright (c) 2019 Fuseinfo Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package com.fuseinfo.jets.util;

import java.util.Map;

public class CommandUtils {
    public static String[] parsingArgs(String[] args, Map<String, String> opts, Map<String, String> vars) {
        String option = null;
        int size = args.length;
        for (int i = 0; i < size; i ++) {
            String arg = args[i];
            if (arg.startsWith("--")) {
                if (option != null) opts.put(option, null);
                option = arg.substring(2);
            } else if (arg.length() > 1 && arg.charAt(0) == '-') {
                if (option != null) opts.put(option, null);
                int last = arg.length() - 1;
                for (int j = 1; j < last; j++)
                    opts.put(String.valueOf(arg.charAt(j)), null);
                option = String.valueOf(arg.charAt(last));
            } else if (option != null) {
                opts.put(option, arg);
                option = null;
            } else {
                int idx = arg.indexOf('=');
                if (idx >= 0) {
                    vars.put(arg.substring(0, idx), arg.substring(idx + 1));
                } else {
                    int length = size - i;
                    String[] result = new String[length];
                    System.arraycopy(args, i, result, 0, length);
                    return result;
                }
            }
        }
        return new String[0];
    }

}
