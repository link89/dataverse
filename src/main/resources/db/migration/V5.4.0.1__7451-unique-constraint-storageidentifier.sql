ALTER TABLE dvobject ADD CONSTRAINT unq_dvobject_storageidentifier UNIQUE(owner_id, storageidentifier);
