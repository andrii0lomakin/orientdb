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
package com.orientechnologies.orient.core.metadata.security;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.impl.ODocument;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * Contains the user settings about security and permissions roles.<br>
 * Allowed operation are the classic CRUD, namely:
 *
 * <ul>
 *   <li>CREATE
 *   <li>READ
 *   <li>UPDATE
 *   <li>DELETE
 * </ul>
 *
 * Mode = ALLOW (allow all but) or DENY (deny all but)
 */
@SuppressWarnings("unchecked")
public class ORole extends OIdentity implements OSecurityRole {
  public static final String ADMIN = "admin";
  public static final String CLASS_NAME = "ORole";
  public static final int PERMISSION_NONE = 0;
  public static final int PERMISSION_CREATE = registerPermissionBit(0, "Create");
  public static final int PERMISSION_READ = registerPermissionBit(1, "Read");
  public static final int PERMISSION_UPDATE = registerPermissionBit(2, "Update");
  public static final int PERMISSION_DELETE = registerPermissionBit(3, "Delete");
  public static final int PERMISSION_EXECUTE = registerPermissionBit(4, "Execute");
  public static final int PERMISSION_ALL =
      PERMISSION_CREATE
          + PERMISSION_READ
          + PERMISSION_UPDATE
          + PERMISSION_DELETE
          + PERMISSION_EXECUTE;
  protected static final byte STREAM_DENY = 0;
  protected static final byte STREAM_ALLOW = 1;
  private static final long serialVersionUID = 1L;
  // CRUD OPERATIONS
  private static Int2ObjectOpenHashMap<String> PERMISSION_BIT_NAMES;
  protected ALLOW_MODES mode = ALLOW_MODES.DENY_ALL_BUT;
  protected ORole parentRole;

  private Map<ORule.ResourceGeneric, ORule> rules = new HashMap<ORule.ResourceGeneric, ORule>();

  /** Constructor used in unmarshalling. */
  public ORole() {}

  public ORole(final String iName, final ORole iParent, final ALLOW_MODES iAllowMode) {
    this(iName, iParent, iAllowMode, null);
  }

  public ORole(
      final String iName,
      final ORole iParent,
      final ALLOW_MODES iAllowMode,
      Map<String, OSecurityPolicy> policies) {
    super(CLASS_NAME);
    getDocument().field("name", iName);

    parentRole = iParent;
    getDocument().field("inheritedRole", iParent != null ? iParent.getIdentity() : null);
    if (policies != null) {
      Map<String, OIdentifiable> p = new HashMap<>();
      policies.forEach((k, v) -> p.put(k, ((OSecurityPolicyImpl) v).getElement()));
      getDocument().setProperty("policies", p);
    }

    updateRolesDocumentContent();
  }

  /** Create the role by reading the source document. */
  public ORole(final ODocument iSource) {
    fromStream(iSource);
  }

  /**
   * Convert the permission code to a readable string.
   *
   * @param iPermission Permission to convert
   * @return String representation of the permission
   */
  public static String permissionToString(final int iPermission) {
    int permission = iPermission;
    final StringBuilder returnValue = new StringBuilder(128);
    for (Entry<Integer, String> p : PERMISSION_BIT_NAMES.entrySet()) {
      if ((permission & p.getKey()) == p.getKey()) {
        if (returnValue.length() > 0) returnValue.append(", ");
        returnValue.append(p.getValue());
        permission &= ~p.getKey();
      }
    }
    if (permission != 0) {
      if (returnValue.length() > 0) returnValue.append(", ");
      returnValue.append("Unknown 0x");
      returnValue.append(Integer.toHexString(permission));
    }

    return returnValue.toString();
  }

  public static int registerPermissionBit(final int bitNo, final String iName) {
    if (bitNo < 0 || bitNo > 31)
      throw new IndexOutOfBoundsException(
          "Permission bit number must be positive and less than 32");

    final int value = 1 << bitNo;
    if (PERMISSION_BIT_NAMES == null) {
      PERMISSION_BIT_NAMES = new Int2ObjectOpenHashMap<>();
    }

    if (PERMISSION_BIT_NAMES.containsKey(value))
      throw new IndexOutOfBoundsException("Permission bit number " + bitNo + " already in use");

    PERMISSION_BIT_NAMES.put(value, iName);
    return value;
  }

  @Override
  public void fromStream(final ODocument iSource) {
    if (getDocument() != null) {
      return;
    }

    setDocument(iSource);

    var document = getDocument();
    try {
      final Number modeField = document.field("mode");
      if (modeField == null) {
        mode = ALLOW_MODES.DENY_ALL_BUT;
      } else if (modeField.byteValue() == STREAM_ALLOW) {
        mode = ALLOW_MODES.ALLOW_ALL_BUT;
      } else {
        mode = ALLOW_MODES.DENY_ALL_BUT;
      }

    } catch (Exception ex) {
      OLogManager.instance().error(this, "illegal mode " + ex.getMessage(), ex);
      mode = ALLOW_MODES.DENY_ALL_BUT;
    }

    final OIdentifiable role = document.field("inheritedRole");
    parentRole =
        role != null ? document.getDatabase().getMetadata().getSecurity().getRole(role) : null;

    boolean rolesNeedToBeUpdated = false;
    Object loadedRules = document.field("rules");
    if (loadedRules instanceof Map) {
      loadOldVersionOfRules((Map<String, Number>) loadedRules);
    } else {
      final Set<ODocument> storedRules = (Set<ODocument>) loadedRules;
      if (storedRules != null) {
        for (ODocument ruleDoc : storedRules) {
          final ORule.ResourceGeneric resourceGeneric =
              ORule.ResourceGeneric.valueOf(ruleDoc.<String>field("resourceGeneric"));
          if (resourceGeneric == null) {
            continue;
          }
          final Map<String, Byte> specificResources = ruleDoc.field("specificResources");
          final Byte access = ruleDoc.field("access");

          final ORule rule = new ORule(resourceGeneric, specificResources, access);
          rules.put(resourceGeneric, rule);
        }
      }

      // convert the format of roles presentation to classic one
      rolesNeedToBeUpdated = true;
    }

    if (getName().equals("admin") && !hasRule(ORule.ResourceGeneric.BYPASS_RESTRICTED, null))
      // FIX 1.5.1 TO ASSIGN database.bypassRestricted rule to the role
      addRule(ORule.ResourceGeneric.BYPASS_RESTRICTED, null, ORole.PERMISSION_ALL).save();

    if (rolesNeedToBeUpdated) {
      updateRolesDocumentContent();
      save();
    }
  }

  public boolean allow(
      final ORule.ResourceGeneric resourceGeneric,
      String resourceSpecific,
      final int iCRUDOperation) {
    final ORule rule = rules.get(resourceGeneric);
    if (rule != null) {
      final Boolean allowed = rule.isAllowed(resourceSpecific, iCRUDOperation);
      if (allowed != null) return allowed;
    }

    if (parentRole != null)
      // DELEGATE TO THE PARENT ROLE IF ANY
      return parentRole.allow(resourceGeneric, resourceSpecific, iCRUDOperation);

    return false;
  }

  public boolean hasRule(final ORule.ResourceGeneric resourceGeneric, String resourceSpecific) {
    ORule rule = rules.get(resourceGeneric);

    if (rule == null) return false;

    if (resourceSpecific != null && !rule.containsSpecificResource(resourceSpecific)) return false;

    return true;
  }

  public ORole addRule(
      final ORule.ResourceGeneric resourceGeneric, String resourceSpecific, final int iOperation) {
    ORule rule = rules.get(resourceGeneric);

    if (rule == null) {
      rule = new ORule(resourceGeneric, null, null);
      rules.put(resourceGeneric, rule);
    }

    rule.grantAccess(resourceSpecific, iOperation);

    rules.put(resourceGeneric, rule);

    updateRolesDocumentContent();

    return this;
  }

  @Deprecated
  @Override
  public boolean allow(String iResource, int iCRUDOperation) {
    final String specificResource = ORule.mapLegacyResourceToSpecificResource(iResource);
    final ORule.ResourceGeneric resourceGeneric =
        ORule.mapLegacyResourceToGenericResource(iResource);

    if (specificResource == null || specificResource.equals("*"))
      return allow(resourceGeneric, null, iCRUDOperation);

    return allow(resourceGeneric, specificResource, iCRUDOperation);
  }

  @Deprecated
  @Override
  public boolean hasRule(String iResource) {
    final String specificResource = ORule.mapLegacyResourceToSpecificResource(iResource);
    final ORule.ResourceGeneric resourceGeneric =
        ORule.mapLegacyResourceToGenericResource(iResource);

    if (specificResource == null || specificResource.equals("*"))
      return hasRule(resourceGeneric, null);

    return hasRule(resourceGeneric, specificResource);
  }

  @Deprecated
  @Override
  public OSecurityRole addRule(String iResource, int iOperation) {
    final String specificResource = ORule.mapLegacyResourceToSpecificResource(iResource);
    final ORule.ResourceGeneric resourceGeneric =
        ORule.mapLegacyResourceToGenericResource(iResource);

    if (specificResource == null || specificResource.equals("*"))
      return addRule(resourceGeneric, null, iOperation);

    return addRule(resourceGeneric, specificResource, iOperation);
  }

  @Deprecated
  @Override
  public OSecurityRole grant(String iResource, int iOperation) {
    final String specificResource = ORule.mapLegacyResourceToSpecificResource(iResource);
    final ORule.ResourceGeneric resourceGeneric =
        ORule.mapLegacyResourceToGenericResource(iResource);

    if (specificResource == null || specificResource.equals("*"))
      return grant(resourceGeneric, null, iOperation);

    return grant(resourceGeneric, specificResource, iOperation);
  }

  @Deprecated
  @Override
  public OSecurityRole revoke(String iResource, int iOperation) {
    final String specificResource = ORule.mapLegacyResourceToSpecificResource(iResource);
    final ORule.ResourceGeneric resourceGeneric =
        ORule.mapLegacyResourceToGenericResource(iResource);

    if (specificResource == null || specificResource.equals("*"))
      return revoke(resourceGeneric, null, iOperation);

    return revoke(resourceGeneric, specificResource, iOperation);
  }

  /**
   * Grant a permission to the resource.
   *
   * @return
   */
  public ORole grant(
      final ORule.ResourceGeneric resourceGeneric, String resourceSpecific, final int iOperation) {
    ORule rule = rules.get(resourceGeneric);

    if (rule == null) {
      rule = new ORule(resourceGeneric, null, null);
      rules.put(resourceGeneric, rule);
    }

    rule.grantAccess(resourceSpecific, iOperation);

    rules.put(resourceGeneric, rule);
    updateRolesDocumentContent();
    return this;
  }

  /** Revoke a permission to the resource. */
  public ORole revoke(
      final ORule.ResourceGeneric resourceGeneric, String resourceSpecific, final int iOperation) {
    if (iOperation == PERMISSION_NONE) return this;

    ORule rule = rules.get(resourceGeneric);

    if (rule == null) {
      rule = new ORule(resourceGeneric, null, null);
      rules.put(resourceGeneric, rule);
    }

    rule.revokeAccess(resourceSpecific, iOperation);
    rules.put(resourceGeneric, rule);

    updateRolesDocumentContent();

    return this;
  }

  public String getName() {
    return getDocument().field("name");
  }

  @Deprecated
  public ALLOW_MODES getMode() {
    return mode;
  }

  @Deprecated
  public ORole setMode(final ALLOW_MODES iMode) {
    //    this.mode = iMode;
    //    document.field("mode", mode == ALLOW_MODES.ALLOW_ALL_BUT ? STREAM_ALLOW : STREAM_DENY);
    return this;
  }

  public ORole getParentRole() {
    return parentRole;
  }

  public ORole setParentRole(final OSecurityRole iParent) {
    this.parentRole = (ORole) iParent;
    getDocument().field("inheritedRole", parentRole != null ? parentRole.getIdentity() : null);
    return this;
  }

  @Override
  public ORole save() {
    getDocument().save(ORole.class.getSimpleName());
    return this;
  }

  public Set<ORule> getRuleSet() {
    return new HashSet<ORule>(rules.values());
  }

  @Deprecated
  public Map<String, Byte> getRules() {
    final Map<String, Byte> result = new HashMap<String, Byte>();

    for (ORule rule : rules.values()) {
      String name = ORule.mapResourceGenericToLegacyResource(rule.getResourceGeneric());

      if (rule.getAccess() != null) {
        result.put(name, rule.getAccess());
      }

      for (Map.Entry<String, Byte> specificResource : rule.getSpecificResources().entrySet()) {
        result.put(name + "." + specificResource.getKey(), specificResource.getValue());
      }
    }

    return result;
  }

  @Override
  public String toString() {
    return getName();
  }

  @Override
  public OIdentifiable getIdentity() {
    return getDocument();
  }

  private void loadOldVersionOfRules(final Map<String, Number> storedRules) {
    if (storedRules != null)
      for (Entry<String, Number> a : storedRules.entrySet()) {
        ORule.ResourceGeneric resourceGeneric =
            ORule.mapLegacyResourceToGenericResource(a.getKey());
        ORule rule = rules.get(resourceGeneric);
        if (rule == null) {
          rule = new ORule(resourceGeneric, null, null);
          rules.put(resourceGeneric, rule);
        }

        String specificResource = ORule.mapLegacyResourceToSpecificResource(a.getKey());
        if (specificResource == null || specificResource.equals("*")) {
          rule.grantAccess(null, a.getValue().intValue());
        } else {
          rule.grantAccess(specificResource, a.getValue().intValue());
        }
      }
  }

  private ODocument updateRolesDocumentContent() {
    return getDocument().field("rules", getRules());
  }

  @Override
  public Map<String, OSecurityPolicy> getPolicies() {
    Map<String, OIdentifiable> policies = getDocument().getProperty("policies");
    if (policies == null) {
      return null;
    }
    Map<String, OSecurityPolicy> result = new HashMap<String, OSecurityPolicy>();
    policies
        .entrySet()
        .forEach(
            x -> {
              OElement rec = x.getValue().getRecord();
              if (rec != null) {
                result.put(x.getKey(), new OSecurityPolicyImpl(rec));
              }
            });
    return result;
  }

  @Override
  public OSecurityPolicy getPolicy(String resource) {
    Map<String, OIdentifiable> policies = getDocument().getProperty("policies");
    if (policies == null) {
      return null;
    }
    OIdentifiable entry = policies.get(resource);
    if (entry == null) {
      return null;
    }
    OElement policy = entry.getRecord();
    if (policy == null) {
      return null;
    }
    return new OSecurityPolicyImpl(policy);
  }
}
