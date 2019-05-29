# Fango
Fango, a wrapper for MongoDB to use it as a Object Oriented database.


# Usage Example:

```python
if __name__ == "__main__":
    class Test(FangoObject):
        @FangoStoredField
        def CreationTime(self):
            return datetime.datetime.now()

        @FangoStoredField
        def Data(self):
            return {}

    with connect(dbname='test') as db:
        print "Creating Test Object"
        obj = db.new(Test, 'a', Data={'1': 0})
        obj.insert()
        obj = db.new(Test, Data={'2': 0})
        obj.insert()
        obj2 = obj.copy('c')
        obj2.insert()
```
