package kvraft

type KVStateMachine interface {
	Get(key string) (string, Err)
	Put(key string, value string) Err
	Append(key string, value string) Err
	Clone() KVStateMachine
}

type MemoryKVStateMachine struct {
	Data map[string]string
}

func NewMemoryKVStateMachine() *MemoryKVStateMachine {
	return &MemoryKVStateMachine{
		Data: make(map[string]string),
	}
}

func (m MemoryKVStateMachine) Clone() KVStateMachine {
	newData := make(map[string]string)
	for k, v := range m.Data {
		newData[k] = v
	}
	return &MemoryKVStateMachine{
		Data: newData,
	}
}

func (m MemoryKVStateMachine) Get(key string) (string, Err) {
	if value, ok := m.Data[key]; ok {
		return value, OK
	}
	return "", ErrNoKey
}

func (m MemoryKVStateMachine) Put(key string, value string) Err {
	m.Data[key] = value
	return OK
}

func (m MemoryKVStateMachine) Append(key string, value string) Err {
	if _, ok := m.Data[key]; !ok {
		m.Data[key] = ""
	}
	m.Data[key] += value
	return OK
}
