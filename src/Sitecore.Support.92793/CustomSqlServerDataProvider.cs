namespace Sitecore.Support.Data.SqlServer
{
    using Sitecore.Data;
    using Sitecore.Data.DataProviders;
    using Sitecore.Data.DataProviders.Sql;
    using Sitecore.Data.Items;
    using Sitecore.Data.SqlServer;
    using Sitecore.SecurityModel;
    using System;
    using System.Linq;
    using System.Reflection;

    public class CustomSqlServerDataProvider : SqlServerDataProvider
    {
        public CustomSqlServerDataProvider(string connectionString) : base(connectionString)
        {
        }

        private bool IsLastVersionLanguage(ItemDefinition itemDefinition, VersionUri version, CallContext context)
        {
            Func<Item, bool> predicate = null;
            using (new SecurityDisabler())
            {
                if (predicate == null)
                {
                    predicate = x => x.Language == version.Language;
                }
                return
                (context.DataManager.Database.GetItem(itemDefinition.ID)
                     .Versions.GetVersions(true)
                     .Count<Item>(predicate) == 1);
            }
        }

        public override bool RemoveVersion(ItemDefinition itemDefinition, VersionUri version, CallContext context)
        {
            lock (this.GetLock(itemDefinition.ID))
            {
                string sql =
                    " DELETE FROM {0}VersionedFields{1}\r\n                        WHERE {0}ItemId{1} = {2}itemId{3}\r\n                        AND {0}Version{1} = {2}version{3}\r\n                        AND {0}Language{1} = {2}language{3}";
                base.Api.Execute(sql,
                    new object[] {"itemId", itemDefinition.ID, "language", version.Language, "version", version.Version});
                if (this.IsLastVersionLanguage(itemDefinition, version, context))
                {
                    sql =
                        "DELETE FROM {0}UnversionedFields{1}\r\n                  WHERE {0}ItemId{1} = {2}itemId{3}\r\n                  AND {0}Language{1} = {2}language{3}\r\n";
                    base.Api.Execute(sql, new object[] {"itemId", itemDefinition.ID, "language", version.Language});
                }
            }
            base.RemovePrefetchDataFromCache(itemDefinition.ID);
            return true;
        }
    }
}
