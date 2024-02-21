import '../views/database/createRecord.html';
import '../views/database/record.html';
import '../views/widget/form.html';
import '../views/database/edit.html';
import '../views/database/newField.html';
import '../views/database/editDocument.html';
import '../views/document/addLink.html';
import '../views/document/modalConnection.html';
import '../views/database/newFieldPopOver.html';
import Utilities from '../util/library';
import angular from 'angular';

let DocController = angular.module('document.controller', []);
DocController.controller("DocumentEditController", ['$scope', '$injector', '$routeParams', '$location', '$modal', '$q', 'DocumentApi', 'Database', 'Notification', function ($scope, $injector, $routeParams, $location, $modal, $q, DocumentApi, Database, Notification) {

  $injector.invoke(BaseEditController, this, {$scope: $scope});
  Database.setWiki("Edit-Document.html");
  $scope.fixed = Database.header;
  $scope.canSave = true;
  $scope.canDelete = true;
  $scope.canCreate = true;
  $scope.canAdd = true;
  // Toggle modal
  $scope.showModal = function (rid) {
    modalScope = $scope.$new(true);
    modalScope.db = $scope.database;
    modalScope.rid = rid;
    var modalPromise = $modal({
      templateUrl: 'views/document/modalEdit.html',
      persist: true,
      show: false,
      modalClass: 'editEdge',
      scope: modalScope
    });
    $q.when(modalPromise).then(function (modalEl) {
      modalEl.modal('show');
    });
  };

  if (!$scope.doc) {
    $scope.reload();

  } else {

    $scope.headers = Database.getPropertyFromDoc($scope.doc);
    $scope.isGraph = Database.isGraph($scope.doc['@class']);
    if ($scope.outgoings == undefined) {
      $scope.outgoings = new Array;
    }
    $scope.outgoings = $scope.outgoings.concat((Database.getLink($scope.doc)));
  }

  $scope.filterArray = function (arr) {
    if (arr instanceof Array) {
      return arr;
    } else {
      var newArr = new Array;
      newArr.push(arr);
      return newArr;
    }

  }
  $scope.deleteLink = function (group, rid) {
    var index = $scope.doc[group].indexOf(rid);
    $scope.doc[group].splice(index, 1);
  }

  $scope.showModalConnection = function (label) {
    var modalScope = $scope.$new(true);
    modalScope.type = Database.listPropertyForClass($scope.doc['@class'], label);
    modalScope.db = $scope.database;
    modalScope.originRid = $scope.rid;
    modalScope.container = $scope;
    modalScope.label = label
    var modalPromise = $modal({
      templateUrl: 'views/document/modalConnection.html',
      persist: true,
      show: true,
      scope: modalScope,
      modalClass: 'createEdge'
    });


  }
}]);
DocController.controller("DocumentCreateController", ['$scope', '$routeParams', '$location', 'DocumentApi', 'Database', 'Notification', function ($scope, $routeParams, $location, DocumentApi, Database, Notification) {


  var database = $routeParams.database;
  var clazz = $routeParams.clazz
  $scope.fixed = Database.header;
  $scope.doc = DocumentApi.createNewDoc(clazz);
  $scope.headers = Database.getPropertyFromDoc($scope.doc);
  $scope.save = function () {
    DocumentApi.createDocument(database, $scope.doc['@rid'], $scope.doc, function (data) {
      Notification.push({content: JSON.stringify(data)});
      $location.path('#/database/' + database + '/browse/edit/' + data['@rid'].replace('#', ''));
    });

  }
}]);
DocController.controller("DocumentModalController", ['$scope', '$routeParams', '$location', 'DocumentApi', 'Database', 'Notification', 'SchemaService', function ($scope, $routeParams, $location, DocumentApi, Database, Notification, SchemaService) {

  $scope.types = Database.getSupportedTypes();

  $scope.database = $scope.db;
  $scope.reload = function () {
    $scope.doc = DocumentApi.get({database: $scope.db, document: $scope.rid}, function () {
      $scope.headers = Database.getPropertyFromDoc($scope.doc,true);
      $scope.onReload();
    }, function (error) {
      Notification.push({content: JSON.stringify(error)});
      $location.path('/404');
    });
  }
  $scope.showClass = function () {
    $scope.$hide();
  }


  $scope.onReload = function () {

  }

  $scope.save = function () {

    if ($scope.isNew) {
      DocumentApi.createDocument($scope.db, $scope.doc['@rid'], $scope.doc, function (data) {
        if ($scope.confirmSave) {
          $scope.confirmSave(data)
        } else {
          Notification.push({content: JSON.stringify(data)});
          $location.path('#/database/' + $scope.db + '/browse/edit/' + data['@rid'].replace('#', ''));
        }

      });
    } else {
      DocumentApi.updateDocument($scope.db, $scope.rid, $scope.doc, function (data) {

        if ($scope.confirmSave) {
          if ($scope.confirmSave(data));
        } else {
          Notification.push({content: data});
        }
      });
    }

  }
  $scope.addField = function (name, type) {
    if (name) {


      $scope.doc[name] = null;
      var types = $scope.doc['@fieldTypes'];
      if (types) {
        var mapping = Database.getMappingFor(type);
        if (mapping)
          types = types + ',' + name + '=' + mapping;
      } else {
        var mapping = Database.getMappingFor(type);
        if (mapping)
          types = name + '=' + mapping;
      }
      $scope.doc['@fieldTypes'] = types;
      $scope.$broadcast('fieldAdded', name);
      $scope.headers.push(name);
    }
  }
  $scope.setSelectClass = function (cls) {
    $scope.doc = DocumentApi.createNewDoc(cls);

    $scope.headers = Database.getPropertyFromDoc($scope.doc);
    $scope.selectClass = false;

  }

  $scope.fillClasses = function (classes) {
    return SchemaService.vertexClasses(classes).map((c) => c.name);
  }

  $scope.deleteField = function (name) {
    delete $scope.doc[name];
    var idx = $scope.headers.indexOf(name);
    $scope.headers.splice(idx, 1);
  }
  if (!$scope.doc && !$scope.isNew) {
    $scope.selectClass = false;
    $scope.reload();
  } else {
    $scope.selectClass = true;
    Database.refreshMetadata($scope.db, () => {
      $scope.listClasses = $scope.fillClasses(Database.listClasses());
      if ($scope.listClasses.length == 1) {
        $scope.selectedClass = $scope.listClasses[0];
      }
    });

  }

}]);


DocController.controller("DocumentModalEdgeController", ['$scope', '$routeParams', '$location', 'CommandApi', 'DocumentApi', 'Database', 'Notification', '$controller', 'SchemaService', function ($scope, $routeParams, $location, CommandApi, DocumentApi, Database, Notification, $controller, SchemaService) {

  $controller('DocumentModalController', {$scope: $scope});


  $scope.fillClasses = function (classes) {
    return SchemaService.edgeClasses(classes).map((c) => c.name);
  }

  $scope.lightweight = false;
  $scope.save = function (cls) {

    if (!cls) {
      cls = $scope.doc["@class"];
      delete $scope.doc["@class"];
      delete $scope.doc["@rid"];
      delete $scope.doc["@version"];
    }

    var params = {
      label: cls,
      source: $scope.source["@rid"],
      target: $scope.target["@rid"],
      json: JSON.stringify($scope.doc)
    };
    var command = "CREATE EDGE `{{label}}` FROM {{source}} TO {{target}}"
    command += " content {{json}}";

    var queryText = S(command).template(params).s;

    CommandApi.queryText({
      database: $routeParams.database,
      language: 'sql',
      text: queryText,
      verbose: false
    }, function (data) {
      $scope.confirmSave(data.result);
      $scope.$hide();
    }, function (err) {
      $scope.$hide();
      $scope.cancelSave(err);
    });
  }


  $scope.setSelectClass = function (cls) {
    $scope.doc = DocumentApi.createNewDoc(cls);

    delete $scope.doc["in"];
    delete $scope.doc["out"];

    $scope.headers = $scope.headers = Database.getPropertyFromDoc($scope.doc).filter((c) => {
      return c != "in" && c != "out";
    });
    $scope.selectClass = false;


  }


  $scope.createLightEdge = function (cls) {
    $scope.save(cls);
    $scope.$hide();
  }
  $scope.showEdgeForm = function () {
    return $scope.selectClass && !$scope.lightweight;
  }
}]);
DocController.controller("EditController", ['$scope', '$routeParams', '$location', 'DocumentApi', 'Database', 'Notification', 'SchemaService', function ($scope, $routeParams, $location, DocumentApi, Database, Notification, SchemaService) {

  var database = $routeParams.database;
  var rid = $routeParams.rid;
  $scope.doc = DocumentApi.get({database: database, document: rid}, function () {

    let classes = Database.listClasses();
    if (SchemaService.isVertexClass(classes, $scope.doc['@class'])) {
      $scope.template = 'views/database/editVertex.html';
    } else if (SchemaService.isEdgeClass(classes, $scope.doc['@class'])) {
      $scope.template = 'views/database/editEdge.html';
    } else {
      $scope.template = 'views/database/editDocument.html';
    }
  }, function (error) {
    Notification.push({content: JSON.stringify(error)});
    $location.path('404');
  });


}]);
DocController.controller("CreateController", ['$scope', '$routeParams', '$location', 'DocumentApi', 'Database', 'Notification', '$timeout', 'SchemaService', function ($scope, $routeParams, $location, DocumentApi, Database, Notification, $timeout, SchemaService) {

  var database = $routeParams.database;
  var clazz = $routeParams.clazz
  $scope.fixed = Database.header;
  $scope.doc = DocumentApi.createNewDoc(clazz);
  $scope.headers = Database.getPropertyFromDoc($scope.doc);
  $scope.isNew = true;

  let classes = Database.listClasses();
  if (SchemaService.isVertexClass(classes, clazz)) {
    $scope.template = 'views/database/editVertex.html';
  } else if (SchemaService.isEdgeClass(classes, clazz)) {
    $scope.template = 'views/database/editEdge.html';
  } else {
    $scope.template = 'views/database/editDocument.html';
  }

}]);
DocController.controller("DocumentModalBrowseController", ['$scope', '$routeParams', '$location', 'Database', 'CommandApi', '$timeout', function ($scope, $routeParams, $location, Database, CommandApi, $timeout) {

  $scope.database = Database;
  $scope.limit = 20;
  $scope.queries = new Array;
  $scope.added = new Array;
  $scope.queryText = $scope.type ? "select * from " + $scope.type.linkedClass : "";
  $scope.editorOptions = {
    lineWrapping: true,
    lineNumbers: true,
    readOnly: false,
    mode: 'text/x-sql',
    metadata: Database,
    extraKeys: {
      "Ctrl-Enter": function (instance) {
        $scope.$apply(function () {
          $scope.query();
        });
      },
      "Ctrl-Space": "autocomplete"
    },
    onLoad: function (_cm) {
      $scope.cm = _cm;

      $scope.cm.on("change", function () { /* script */
        var wrap = $scope.cm.getWrapperElement();
        var approp = $scope.cm.getScrollInfo().height > 300 ? "300px" : "auto";
        if (wrap.style.height != approp) {
          wrap.style.height = approp;
          $scope.cm.refresh();
        }
      });
      $scope.cm.refresh();
    }
  };
  $scope.query = function () {
    CommandApi.queryText({
      database: $routeParams.database,
      language: 'sql',
      text: $scope.queryText,
      limit: $scope.limit,
      verbose: false
    }, function (data) {
      if (data.result) {
        $scope.headers = Database.getPropertyTableFromResults(data.result);
        $scope.results = data.result;
      }
      if ($scope.queries.indexOf($scope.queryText) == -1)
        $scope.queries.push($scope.queryText);
    }, function err(data) {
      $scope.error = data;
      $timeout(function () {
        $scope.error = null;
      }, 2000);
    });
  }
  $scope.select = function (result) {
    var index = $scope.added.indexOf(result['@rid']);
    if (index == -1) {
      $scope.added.push(result['@rid']);
    } else {
      $scope.added.splice(index, 1);
    }
  }
  $scope.createLink = function () {
    if (!$scope.container.doc[$scope.label]) {
      $scope.container.doc[$scope.label] = new Array;
    }
    $scope.container.doc[$scope.label] = $scope.container.doc[$scope.label].concat($scope.added);
    $scope.container.save();

  }
}]);

DocController.controller("DocumentPopoverLinkController", ['$scope', '$routeParams', '$location', 'DocumentApi', 'Database', 'Notification', function ($scope, $routeParams, $location, DocumentApi, Database, Notification) {


  $scope.addLink = function (name) {
    if ($scope['outgoings'].indexOf(name) == -1) {

      $scope['outgoings'].push(name);
      var types = $scope.doc['@fieldTypes']
      if (types) {
        types = types + ',' + name + '=e'
      } else {
        types = name + '=e';
      }
      $scope.doc['@fieldTypes'] = types;
    }
  }

}]);
function BaseEditController($scope, $routeParams, $route, $location, $modal, $q, DocumentApi, Database, Notification, CommandApi) {
  $scope.database = $routeParams.database;
  $scope.dbWiki = Database;
  $scope.rid = $routeParams.rid;
  $scope.label = 'Document';


  $scope.save = function () {
    if (!$scope.isNew) {
      DocumentApi.updateDocument($scope.database, $scope.rid, $scope.doc).then((data) => {
        Notification.push({content: JSON.stringify(data)});
        $route.reload();
      }).catch((err) => {
        Notification.push({content: err, error: true});
      });
    } else {
      DocumentApi.createDocument($scope.database, $scope.doc['@rid'], $scope.doc, function (data) {
        Notification.push({content: JSON.stringify(data)});
        $location.path('database/' + $scope.database + '/browse/edit/' + data['@rid'].replace('#', ''));
      }, (err) => {
        Notification.push({content: err, error: true, autoHide: true});
      });
    }
  }


  $scope.reload = function () {

    $scope.doc = DocumentApi.get({database: $scope.database, document: $scope.rid}, function () {
      /*$scope.headers = Database.getPropertyFromDoc($scope.doc);
       $scope.isGraph = Database.isGraph($scope.doc['@class']);
       $scope.incomings = Database.getEdge($scope.doc, 'in_');
       $scope.outgoings = Database.getEdge($scope.doc, 'out_');
       $scope.outgoings = $scope.outgoings.concat((Database.getLink($scope.doc)));*/
    }, function (error) {
      Notification.push({content: JSON.stringify(error)});
      $location.path('404');
    });
  }

  $scope.getLabelFor = function (label) {
    var props = Database.listPropertyForClass($scope.doc['@class'], label);
    var propsLabel = props != null ? (props.linkedClass != undefined ? " (" + (props.linkedClass) + ")" : "" ) : "";
    return label + propsLabel;
  }
  $scope.delete = function () {

    var recordID = $scope.doc['@rid'];
    var clazz = $scope.doc['@class'];
    Utilities.confirm($scope, $modal, $q, {
      title: 'Warning!',
      body: 'You are removing ' + $scope.label + ' ' + recordID + '. Are you sure?',
      success: function () {
        var command = "DELETE Vertex " + recordID;
        DocumentApi.deleteDocument($scope.database, recordID, function (data) {
          var clazz = $scope.doc['@class'];
          $location.path('/database/' + $scope.database + '/browse/' + 'select * from ' + clazz);
        });
      }
    });
  }

  $scope.deleteField = function (name) {
    Utilities.confirm($scope, $modal, $q, {
      title: 'Warning!',
      body: 'You are removing field ' + name + ' from ' + $scope.label + ' ' + $scope.doc['@rid'] + '. Are you sure?',
      success: function () {
        delete $scope.doc[name];
        var idx = $scope.headers.indexOf(name);
        $scope.headers.splice(idx, 1);
      }
    });
  }

  $scope.addField = function (name, type) {
    if (name) {
      $scope.doc[name] = null;
      var types = $scope.doc['@fieldTypes'];
      if (type == 'BOOLEAN') {
        $scope.doc[name] = false;
      }
      if (type == 'INTEGER') {
        $scope.doc[name] = 0;
      }
      if (type == "EMBEDDED" || type == "EMBEDDEDMAP") {
        $scope.doc[name] = {};
      }
      if (Database.getMappingFor(type)) {
        if (types) {
          types = types + ',' + name + '=' + Database.getMappingFor(type);
        } else {
          types = name + '=' + Database.getMappingFor(type);
        }
        $scope.doc['@fieldTypes'] = types;
      }

      $scope.$broadcast('fieldAdded', name);
      $scope.headers.push(name);
    } else {
      var modalScope = $scope.$new(true);
      modalScope.addField = $scope.addField;
      modalScope.types = Database.getSupportedTypes();
      var modalPromise = $modal({
        templateUrl: 'views/database/newField.html',
        persist: true,
        show: true,
        scope: modalScope
      });

    }

  }
  $scope.follow = function (rid) {
    $scope.navigate(rid);
  }
  $scope.navigate = function (rid) {
    $location.path('database/' + $scope.database + '/browse/edit/' + rid.replace('#', ''));
  }
  $scope.create = function () {
    $location.path('database/' + $scope.database + '/browse/create/' + $scope.doc['@class']);
  }
}
DocController.controller("EmbeddedController", ['$scope', '$ojson', function ($scope, $ojson) {


  $scope.viewerOptions = {
    lineWrapping: true,
    lineNumbers: true,
    mode: 'javascript',
    onLoad: function (_cm) {
      $scope.vcm = _cm;
    }

  };

}]);


const CtrlName = DocController.name;
export  {CtrlName, BaseEditController};

