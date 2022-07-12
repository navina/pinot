/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.tools;

import com.google.common.base.Preconditions;
import java.io.File;
import java.util.List;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.index.readers.BaseImmutableDictionary;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.utils.ReadMode;
import picocli.CommandLine;


@CommandLine.Command
public class DictionaryDumper extends AbstractBaseCommand implements Command {
  @CommandLine.Option(names = {"-path"}, required = true,
      description = "Path of the folder containing the segment" + " file")
  private String _segmentDir = null;

  @CommandLine.Option(names = {"-dimensionName"}, description = "Dimension Name")
  private String _dimensionName;

  @CommandLine.Option(names = {"-dictIds"}, arity = "1..*", description = "Dictionary Ids to dump")
  private List<String> _dictIds;

  @CommandLine.Option(names = {"-lengthLowerBound"}, description = "lower bound on the length of the "
      + "dictionary string that will be printed")
  private int _lengthLowerBound = -1;


  public void doMain(String[] args)
      throws Exception {
    CommandLine commandLine = new CommandLine(this);
    commandLine.parseArgs(args);
    dumpDict();
  }

  private void dumpDict()
      throws Exception {
    File[] indexDirs = new File(_segmentDir).listFiles();
    Preconditions.checkNotNull(indexDirs);

    for (File indexDir : indexDirs) {
      System.out.println("Loading " + indexDir.getName());

      ImmutableSegment immutableSegment = ImmutableSegmentLoader.load(indexDir, ReadMode.mmap);
      int colMaxLength =
          immutableSegment.getSegmentMetadata().getColumnMetadataFor(_dimensionName).getColumnMaxLength();

      Dictionary colDictionary = immutableSegment.getDictionary(_dimensionName);
      if (_dictIds != null) {
        for (String strId : _dictIds) {
          int id = Integer.valueOf(strId);
          String s = colDictionary.getStringValue(id);
          System.out.println(String.format("%d -> %s", id, s));
        }
      } else {
        int dictLength = colDictionary.length();
        System.out.println("Dictionary length for dimension " + _dimensionName + " - " + dictLength);
        byte[] buffer = new byte[colMaxLength];
        for (int i = 0; i < dictLength; i++) {
          String s = ((BaseImmutableDictionary) colDictionary).getPaddedString(i, buffer);
          String replaced = s.replaceAll("\0", "");
            System.out.println(i + "\t" + replaced.length());
        }
      }
    }
  }

  public static void main(String[] args)
      throws Exception {
    new DictionaryDumper().doMain(args);
  }

  public String getName() {
    return getClass().getSimpleName();
  }

  @Override
  public boolean execute()
      throws Exception {
    dumpDict();
    return true;
  }

  @Override
  public String description() {
    return "Dump the dictionary content for a given dimension";
  }

  @Override
  public boolean getHelp() {
    return false;
  }
}
