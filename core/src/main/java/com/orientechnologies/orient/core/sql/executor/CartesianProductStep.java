package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.executor.resultset.OIteratorResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

/** Created by luigidellaquila on 11/10/16. */
public class CartesianProductStep extends AbstractExecutionStep {

  private List<OInternalExecutionPlan> subPlans = new ArrayList<>();
  private long cost = 0;

  public CartesianProductStep(OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
  }

  @Override
  public OResultSet syncPull(OCommandContext ctx) throws OTimeoutException {
    getPrev().ifPresent(x -> x.syncPull(ctx));
    Stream<List<OResult>> stream = null;
    for (OInternalExecutionPlan ep : this.subPlans) {
      if (stream == null) {
        stream =
            ep.fetchNext().stream()
                .map(
                    (value) -> {
                      List<OResult> result = new ArrayList<>();
                      result.add(value);
                      return result;
                    });
      } else {
        stream =
            stream.flatMap(
                (val) -> {
                  return ep.fetchNext().stream()
                      .map(
                          (value) -> {
                            List<OResult> result = new ArrayList<>(val);
                            result.add(value);
                            return result;
                          });
                });
      }
    }
    Stream<OResult> finalStream =
        stream.map(
            (path) -> {
              long begin = profilingEnabled ? System.nanoTime() : 0;
              try {

                OResultInternal nextRecord = new OResultInternal();

                for (int i = 0; i < path.size(); i++) {
                  OResult res = path.get(i);
                  for (String s : res.getPropertyNames()) {
                    nextRecord.setProperty(s, res.getProperty(s));
                  }
                }
                return nextRecord;
              } finally {
                if (profilingEnabled) {
                  cost += (System.nanoTime() - begin);
                }
              }
            });
    return new OIteratorResultSet(finalStream.iterator());
  }

  public void addSubPlan(OInternalExecutionPlan subPlan) {
    this.subPlans.add(subPlan);
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String result = "";
    String ind = OExecutionStepInternal.getIndent(depth, indent);

    int[] blockSizes = new int[subPlans.size()];

    for (int i = 0; i < subPlans.size(); i++) {
      OInternalExecutionPlan currentPlan = subPlans.get(subPlans.size() - 1 - i);
      String partial = currentPlan.prettyPrint(0, indent);

      String[] partials = partial.split("\n");
      blockSizes[subPlans.size() - 1 - i] = partials.length + 2;
      result = "+-------------------------\n" + result;
      for (int j = 0; j < partials.length; j++) {
        String p = partials[partials.length - 1 - j];
        if (result.length() > 0) {
          result = appendPipe(p) + "\n" + result;
        } else {
          result = appendPipe(p);
        }
      }
      result = "+-------------------------\n" + result;
    }
    result = addArrows(result, blockSizes);
    result += foot(blockSizes);
    result = ind + result;
    result = result.replaceAll("\n", "\n" + ind);
    result = head(depth, indent, subPlans.size()) + "\n" + result;
    return result;
  }

  private String addArrows(String input, int[] blockSizes) {
    StringBuilder result = new StringBuilder();
    String[] rows = input.split("\n");
    int rowNum = 0;
    for (int block = 0; block < blockSizes.length; block++) {
      int blockSize = blockSizes[block];
      for (int subRow = 0; subRow < blockSize; subRow++) {
        for (int col = 0; col < blockSizes.length * 3; col++) {
          if (isHorizontalRow(col, subRow, block, blockSize)) {
            result.append("-");
          } else if (isPlus(col, subRow, block, blockSize)) {
            result.append("+");
          } else if (isVerticalRow(col, subRow, block, blockSize)) {
            result.append("|");
          } else {
            result.append(" ");
          }
        }
        result.append(rows[rowNum]);
        result.append("\n");
        rowNum++;
      }
    }

    return result.toString();
  }

  private boolean isHorizontalRow(int col, int subRow, int block, int blockSize) {
    if (col < block * 3 + 2) {
      return false;
    }
    if (subRow == blockSize / 2) {
      return true;
    }
    return false;
  }

  private boolean isPlus(int col, int subRow, int block, int blockSize) {
    if (col == block * 3 + 1) {
      if (subRow == blockSize / 2) {
        return true;
      }
    }
    return false;
  }

  private boolean isVerticalRow(int col, int subRow, int block, int blockSize) {
    if (col == block * 3 + 1) {
      if (subRow > blockSize / 2) {
        return true;
      }
    } else if (col < block * 3 + 1 && col % 3 == 1) {
      return true;
    }

    return false;
  }

  private String head(int depth, int indent, int nItems) {
    String ind = OExecutionStepInternal.getIndent(depth, indent);
    String result = ind + "+ CARTESIAN PRODUCT";
    if (profilingEnabled) {
      result += " (" + getCostFormatted() + ")";
    }
    return result;
  }

  private String foot(int[] blockSizes) {
    StringBuilder result = new StringBuilder();
    for (int i = 0; i < blockSizes.length; i++) {
      result.append(" V ");
    }
    return result.toString();
  }

  private String appendPipe(String p) {
    return "| " + p;
  }

  @Override
  public long getCost() {
    return cost;
  }
}
