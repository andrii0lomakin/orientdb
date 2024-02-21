let browseConfig = angular.module('browse.services', []);

browseConfig.factory('BrowseConfig', ["$rootScope", "Database", "$location", "localStorageService", "$timeout", function ($rootScope, Database, $location, localStorageService, $timeout) {


  let pageSize = localStorageService.get('pageSize') || 10;
  var config = {
    limit: localStorageService.get('limit') || 20,
    hideSettings: true,
    keepLimit: '10',
    pageSize: parseInt(pageSize),
    selectedContentType: 'JSON',
    selectedRequestType: 'COMMAND',
    selectedRequestLanguage: 'SQL',
    set: function (name, val) {
      this[name] = val;
      localStorageService.add(name, val);
    },
    get: function (name) {
      return this[name];
    },
    storageSize: function (ms, cb) {
      $timeout(function () {
        cb(unescape(encodeURIComponent(JSON.stringify(localStorage))).length);
      }, ms);
    }
  };


  return config;
}]);

export default browseConfig.name;
