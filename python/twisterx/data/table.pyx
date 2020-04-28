from libcpp.string cimport string
from twisterx.common.status cimport _Status
from pytwisterx.common.status import Status
import uuid
from twisterx.common.join_config cimport CJoinType
from twisterx.common.join_config cimport CJoinAlgorithm
from twisterx.common.join_config cimport CJoinConfig
from pytwisterx.common.join.config import PJoinType
from pytwisterx.common.join.config import PJoinAlgorithm

cdef extern from "../../../cpp/src/twisterx/python/table_cython.h" namespace "twisterx::python::table":
    cdef cppclass CTable "twisterx::python::table::CTable":
        CTable()
        CTable(string)
        string get_id()
        int columns()
        int rows()
        void show()
        void show(int, int, int, int)
        _Status to_csv(const string)
        string join(const string, CJoinType, CJoinAlgorithm, int, int)
        string join(const string, CJoinConfig)

cdef extern from "../../../cpp/src/twisterx/python/table_cython.h" namespace "twisterx::python::table::CTable":
    cdef extern _Status from_csv(const string, const char, const string)



cdef class Table:
    cdef CTable *thisPtr
    cdef CJoinConfig *jcPtr

    def __cinit__(self, string id):
        self.thisPtr = new CTable(id)
        #self.tablePtr = make_unique[CTable]()

    cdef __get_join_config(self, join_type: str, join_algorithm: str, left_column_index: int,
                           right_column_index: int):
        if left_column_index is None or right_column_index is None:
            raise Exception("Join Column index not provided")

        if join_algorithm is None:
            join_algorithm = PJoinAlgorithm.HASH.value

        if join_algorithm == PJoinAlgorithm.HASH.value:

            if join_type == PJoinType.INNER.value:
                self.jcPtr = new CJoinConfig(CJoinType.INNER, left_column_index, right_column_index,
                                             CJoinAlgorithm.HASH)
            elif join_type == PJoinType.LEFT.value:
                self.jcPtr = new CJoinConfig(CJoinType.LEFT, left_column_index, right_column_index,
                                             CJoinAlgorithm.HASH)
            elif join_type == PJoinType.RIGHT.value:
                self.jcPtr = new CJoinConfig(CJoinType.RIGHT, left_column_index, right_column_index,
                                             CJoinAlgorithm.HASH)
            elif join_type == PJoinType.OUTER.value:
                self.jcPtr = new CJoinConfig(CJoinType.OUTER, left_column_index, right_column_index,
                                             CJoinAlgorithm.HASH)
            else:
                raise ValueError("Unsupported Join Type {}".format(join_type))

        elif join_algorithm == PJoinAlgorithm.SORT.value:

            if join_type == PJoinType.INNER.value:
                self.jcPtr = new CJoinConfig(CJoinType.INNER, left_column_index, right_column_index,
                                             CJoinAlgorithm.SORT)
            elif join_type == PJoinType.LEFT.value:
                self.jcPtr = new CJoinConfig(CJoinType.LEFT, left_column_index, right_column_index,
                                             CJoinAlgorithm.SORT)
            elif join_type == PJoinType.RIGHT.value:
                self.jcPtr = new CJoinConfig(CJoinType.RIGHT, left_column_index, right_column_index,
                                             CJoinAlgorithm.SORT)
            elif join_type == PJoinType.OUTER.value:
                self.jcPtr = new CJoinConfig(CJoinType.OUTER, left_column_index, right_column_index,
                                             CJoinAlgorithm.SORT)
            else:
                raise ValueError("Unsupported Join Type {}".format(join_type))
        else:
            if join_type == PJoinType.INNER.value:
                self.jcPtr = new CJoinConfig(CJoinType.INNER, left_column_index, right_column_index)
            elif join_type == PJoinType.LEFT.value:
                self.jcPtr = new CJoinConfig(CJoinType.LEFT, left_column_index, right_column_index)
            elif join_type == PJoinType.RIGHT.value:
                self.jcPtr = new CJoinConfig(CJoinType.RIGHT, left_column_index, right_column_index)
            elif join_type == PJoinType.OUTER.value:
                self.jcPtr = new CJoinConfig(CJoinType.OUTER, left_column_index, right_column_index)
            else:
                raise ValueError("Unsupported Join Type {}".format(join_type))

    @property
    def id(self) -> str:
        return self.thisPtr.get_id().decode()

    @property
    def columns(self) -> str:
        return self.thisPtr.columns()

    @property
    def rows(self) -> str:
        return self.thisPtr.rows()

    def show(self):
        self.thisPtr.show()

    def show_by_range(self, row1: int, row2: int, col1: int, col2: int):
        self.thisPtr.show(row1, row2, col1, col2)

    def to_csv(self, path: str) -> Status:
        cdef _Status status = self.thisPtr.to_csv(path.encode())
        s = Status(status.get_code(), b"", -1)
        return s

    def join(self, table: Table, join_type: str, algorithm: str, left_col: int, right_col: int) -> Table:
        self.__get_join_config(join_type=join_type, join_algorithm=algorithm, left_column_index=left_col,
                                right_column_index=right_col)
        cdef CJoinConfig *jc1 = self.jcPtr
        cdef string table_out_id = self.thisPtr.join(table.id.encode(), jc1[0])
        if table_out_id.size() == 0:
            raise Exception("Join Failed !!!")
        return Table(table_out_id)

cdef class csv:
    #cdef _Table *thisPtr
    #cdef unique_ptr[_Table] tablePtr

    @staticmethod
    def read(path: str, delimiter: str) -> Table:
        cdef string spath = path.encode()
        cdef string sdelm = delimiter.encode()
        id = uuid.uuid4()
        id_str = id.__str__()
        id_buf = id_str.encode()
        from_csv(spath, sdelm[0], id_buf)
        id_buf = id_str.encode()
        return Table(id_buf)