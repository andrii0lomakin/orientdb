/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://orientdb.com
 *
 */

package com.orientechnologies.orient.core.index;

import com.orientechnologies.orient.core.collate.OCollate;
import com.orientechnologies.orient.core.collate.ODefaultCollate;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OSQLEngine;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nonnull;

public class OSimpleKeyIndexDefinition extends OAbstractIndexDefinition {
  private OType[] keyTypes;

  public OSimpleKeyIndexDefinition(final OType... keyTypes) {
    super();

    this.keyTypes = keyTypes;
  }

  public OSimpleKeyIndexDefinition() {}

  public OSimpleKeyIndexDefinition(OType[] keyTypes2, List<OCollate> collatesList) {
    super();

    this.keyTypes = Arrays.copyOf(keyTypes2, keyTypes2.length);

    if (keyTypes.length > 1) {
      OCompositeCollate collate = new OCompositeCollate(this);
      if (collatesList != null) {
        for (OCollate oCollate : collatesList) {
          collate.addCollate(oCollate);
        }
      } else {
        final int typesSize = keyTypes.length;
        final OCollate defCollate = OSQLEngine.getCollate(ODefaultCollate.NAME);
        for (int i = 0; i < typesSize; i++) {
          collate.addCollate(defCollate);
        }
      }
      this.collate = collate;
    }
  }

  public List<String> getFields() {
    return Collections.emptyList();
  }

  public List<String> getFieldsToIndex() {
    return Collections.emptyList();
  }

  public String getClassName() {
    return null;
  }

  public Object createValue(final List<?> params) {
    return createValue(params != null ? params.toArray() : null);
  }

  public Object createValue(final Object... params) {
    if (params == null || params.length == 0) return null;

    if (keyTypes.length == 1) return OType.convert(params[0], keyTypes[0].getDefaultJavaType());

    final OCompositeKey compositeKey = new OCompositeKey();

    for (int i = 0; i < params.length; ++i) {
      final Comparable<?> paramValue =
          (Comparable<?>) OType.convert(params[i], keyTypes[i].getDefaultJavaType());

      if (paramValue == null) return null;
      compositeKey.addKey(paramValue);
    }

    return compositeKey;
  }

  public int getParamCount() {
    return keyTypes.length;
  }

  public OType[] getTypes() {
    return Arrays.copyOf(keyTypes, keyTypes.length);
  }

  @Override
  public @Nonnull ODocument toStream(@Nonnull ODocument document) {
    serializeToStream(document);
    return document;
  }

  @Override
  protected void serializeToStream(ODocument document) {
    super.serializeToStream(document);

    final List<String> keyTypeNames = new ArrayList<>(keyTypes.length);

    for (final OType keyType : keyTypes) keyTypeNames.add(keyType.toString());

    document.field("keyTypes", keyTypeNames, OType.EMBEDDEDLIST);
    if (collate instanceof OCompositeCollate) {
      List<String> collatesNames = new ArrayList<>();
      for (OCollate curCollate : ((OCompositeCollate) this.collate).getCollates())
        collatesNames.add(curCollate.getName());
      document.field("collates", collatesNames, OType.EMBEDDEDLIST);
    } else document.field("collate", collate.getName());

    document.field("nullValuesIgnored", isNullValuesIgnored());
  }

  @Override
  public void fromStream(@Nonnull ODocument document) {
    serializeFromStream(document);
  }

  @Override
  protected void serializeFromStream(ODocument document) {
    super.serializeFromStream(document);

    final List<String> keyTypeNames = document.field("keyTypes");
    keyTypes = new OType[keyTypeNames.size()];

    int i = 0;
    for (final String keyTypeName : keyTypeNames) {
      keyTypes[i] = OType.valueOf(keyTypeName);
      i++;
    }
    String collate = document.field("collate");
    if (collate != null) {
      setCollate(collate);
    } else {
      final List<String> collatesNames = document.field("collates");
      if (collatesNames != null) {
        OCompositeCollate collates = new OCompositeCollate(this);
        for (String collateName : collatesNames) {
          collates.addCollate(OSQLEngine.getCollate(collateName));
        }
        this.collate = collates;
      }
    }

    setNullValuesIgnored(!Boolean.FALSE.equals(document.<Boolean>field("nullValuesIgnored")));
  }

  public Object getDocumentValueToIndex(final ODocument iDocument) {
    throw new OIndexException("This method is not supported in given index definition.");
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    final OSimpleKeyIndexDefinition that = (OSimpleKeyIndexDefinition) o;
    return Arrays.equals(keyTypes, that.keyTypes);
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + (keyTypes != null ? Arrays.hashCode(keyTypes) : 0);
    return result;
  }

  @Override
  public String toString() {
    return "OSimpleKeyIndexDefinition{"
        + "keyTypes="
        + (keyTypes == null ? null : Arrays.asList(keyTypes))
        + '}';
  }

  /**
   * {@inheritDoc}
   *
   * @param indexName
   * @param indexType
   */
  public String toCreateIndexDDL(
      final String indexName, final String indexType, final String engine) {
    final StringBuilder ddl = new StringBuilder("create index `");
    ddl.append(indexName).append("` ").append(indexType).append(' ');

    if (keyTypes != null && keyTypes.length > 0) {
      ddl.append(keyTypes[0].toString());
      for (int i = 1; i < keyTypes.length; i++) {
        ddl.append(", ").append(keyTypes[i].toString());
      }
    }
    return ddl.toString();
  }

  @Override
  public boolean isAutomatic() {
    return false;
  }
}
