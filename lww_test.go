package lww

import (
	"github.com/google/go-cmp/cmp"
	"sync"
	"testing"
	"time"
)

var testSuccess = "Success"
var testFailed = "Failed"

// addEl is a helper function for adding element in a multi-thread setting.
func addEl(wg *sync.WaitGroup, lww *LwwSet, el string, timestamp time.Time) {
	lww.Add(el, timestamp)
	wg.Done()
}

// removeEl is a helper function for removing element in a multi-thread setting.
func removeEl(wg *sync.WaitGroup, lww *LwwSet, el string, timestamp time.Time) {
	lww.Remove(el, timestamp)
	wg.Done()
}

func TestStringAddRemove(t *testing.T) {

	t.Log("Given the need to work with some strings on single thread.")
	{
		testID := 0
		t.Logf("\tTest %d:\tWhen handling additions and removals.", testID)
		{
			var a = "s1"
			var b = "s22"
			var now = time.Now()
			var lww = New()

			lww.Add(a, now)
			if !lww.Exist(a) {
				t.Fatalf("\t%s\tTest %d\tShould be able to add a string %s to the lww set.", testFailed, testID, a)
			}
			t.Logf("\t%s\tTest %d:\tShould be able to add a string %s to the lww set.", testSuccess, testID, a)
			if lww.Exist(b) {
				t.Fatalf("\t%s\tTest %d\tShould not be able to find a string %s that doesn't exist in the lww set.", testFailed, testID, b)
			}
			t.Logf("\t%s\tTest %d\tShould not be able to find a string %s that doesn't exist in the lww set.", testSuccess, testID, b)

			lww.Add(b, now)
			lww.Remove(a, now.Add(1000 * time.Millisecond))
			if !lww.Exist(b) {
				t.Fatalf("\t%s\tTest %d\tShould be able to find a string %s added to the lww set.", testFailed, testID, b)
			}
			t.Logf("\t%s\tTest %d\tShould be able to find a string %s added to the lww set.", testSuccess, testID, b)
			if lww.Exist(a) {
				t.Fatalf("\t%s\tTest %d\tShould not be able to find a string %s that is removed from the lww set.", testFailed, testID, a)
			}
			t.Logf("\t%s\tTest %d\tShould not be able to find a string %s that is removed from the lww set.", testSuccess, testID, a)

			var expected = []string{b}
			var results = lww.Get()
			if diff := cmp.Diff(expected, results); diff != "" {
				t.Fatalf("\t%s\tTest %d:\tShould get back correct lww set. Diff:\n%s", testFailed, testID, diff)
			}
			t.Logf("\t%s\tTest %d:\tShould get back correct lww set.", testSuccess, testID)
		}
	}
}

func TestMultiThreaded(t *testing.T) {

	t.Log("Given the need to work with some strings on multi threads.")
	{
		testID := 0
		t.Logf("\tTest %d:\tWhen handling additions and removals.", testID)
		for i := 0; i < 100; i++ {
			var el = "2"
			var now = time.Now()
			var timestamp1 = now.Add(1000 * time.Millisecond)
			var timestamp2 = now.Add(2000 * time.Millisecond)
			var timestamp3 = now.Add(3000 * time.Millisecond)
			var timestamp4 = now.Add(4000 * time.Millisecond)
			var lww = New()

			var wg sync.WaitGroup

			wg.Add(4)
			go addEl(&wg, &lww, el, timestamp3)
			go removeEl(&wg, &lww, el, timestamp1)
			go removeEl(&wg, &lww, el, timestamp2)
			go removeEl(&wg, &lww, el, timestamp4)
			wg.Wait()

			if lww.Exist(el) {
				t.Fatalf("\t%s\tTest %d-%d\tShould not be able to find a string %s that is removed from the lww set.", testFailed, testID, i, el)
			}
			t.Logf("\t%s\tTest %d-%d\tShould not be able to find a string %s that is removed from the lww set.", testSuccess, testID, i, el)
		}
	}
}

func TestTableMultiThread(t *testing.T) {
	t.Log("Given the need to work with the test cases in the README table on multi-thread.")
	{
		testID := 0
		t.Logf("\tTest %d:\tWhen handling add(1, 1), add(1, 0).", testID)
		{
			var now = time.Now()
			var timestamp1 = now.Add(1000 * time.Millisecond)
			var timestamp2 = now.Add(2000 * time.Millisecond)
			var lww = New()

			var wg sync.WaitGroup
			wg.Add(2)
			go addEl(&wg, &lww, "1", timestamp2)
			go addEl(&wg, &lww, "1", timestamp1)
			wg.Wait()

			var expected = []string{"1"}
			var results = lww.Get()
			if diff := cmp.Diff(expected, results); diff != "" {
				t.Fatalf("\t%s\tTest %d:\tShould get back correct lww set: [1]. Diff:\n%s", testFailed, testID, diff)
			}
			t.Logf("\t%s\tTest %d:\tShould get back correct lww set: [1].", testSuccess, testID)
		}

		testID = 1
		t.Logf("\tTest %d:\tWhen handling add(1, 1), add(1, 1).", testID)
		{
			var now = time.Now()
			var timestamp1 = now.Add(1000 * time.Millisecond)
			var lww = New()

			var wg sync.WaitGroup
			wg.Add(2)
			go addEl(&wg, &lww, "1", timestamp1)
			go addEl(&wg, &lww, "1", timestamp1)
			wg.Wait()

			var expected = []string{"1"}
			var results = lww.Get()
			if diff := cmp.Diff(expected, results); diff != "" {
				t.Fatalf("\t%s\tTest %d:\tShould get back correct lww set: [1]. Diff:\n%s", testFailed, testID, diff)
			}
			t.Logf("\t%s\tTest %d:\tShould get back correct lww set: [1].", testSuccess, testID)
		}

		testID = 2
		t.Logf("\tTest %d:\tWhen handling add(1, 1), add(1, 2).", testID)
		{
			var now = time.Now()
			var timestamp1 = now.Add(1000 * time.Millisecond)
			var timestamp2 = now.Add(2000 * time.Millisecond)
			var lww = New()

			var wg sync.WaitGroup
			wg.Add(2)
			go addEl(&wg, &lww, "1", timestamp1)
			go addEl(&wg, &lww, "1", timestamp2)
			wg.Wait()

			var expected = []string{"1"}
			var results = lww.Get()
			if diff := cmp.Diff(expected, results); diff != "" {
				t.Fatalf("\t%s\tTest %d:\tShould get back correct lww set: [1]. Diff:\n%s", testFailed, testID, diff)
			}
			t.Logf("\t%s\tTest %d:\tShould get back correct lww set: [1].", testSuccess, testID)
		}

		testID = 3
		t.Logf("\tTest %d:\tWhen handling remove(1, 1), add(1, 0).", testID)
		{
			var now = time.Now()
			var timestamp1 = now.Add(1000 * time.Millisecond)
			var timestamp2 = now.Add(2000 * time.Millisecond)
			var lww = New()

			var wg sync.WaitGroup
			wg.Add(2)
			go removeEl(&wg, &lww, "1", timestamp2)
			go addEl(&wg, &lww, "1", timestamp1)
			wg.Wait()

			var expected []string
			var results = lww.Get()
			if diff := cmp.Diff(expected, results); diff != "" {
				t.Fatalf("\t%s\tTest %d:\tShould get back correct lww set: []. Diff:\n%s", testFailed, testID, diff)
			}
			t.Logf("\t%s\tTest %d:\tShould get back correct lww set: [].", testSuccess, testID)
		}

		testID = 4
		t.Logf("\tTest %d:\tWhen handling remove(1, 1), add(1, 1).", testID)
		{
			var now = time.Now()
			var timestamp1 = now.Add(1000 * time.Millisecond)
			var lww = New()

			var wg sync.WaitGroup
			wg.Add(2)
			go removeEl(&wg, &lww, "1", timestamp1)
			go addEl(&wg, &lww, "1", timestamp1)
			wg.Wait()

			var expected = []string{"1"}
			var results = lww.Get()
			if diff := cmp.Diff(expected, results); diff != "" {
				t.Fatalf("\t%s\tTest %d:\tShould get back correct lww set: [1]. Diff:\n%s", testFailed, testID, diff)
			}
			t.Logf("\t%s\tTest %d:\tShould get back correct lww set: [1].", testSuccess, testID)
		}

		testID = 5
		t.Logf("\tTest %d:\tWhen handling remove(1, 1), add(1, 2).", testID)
		{
			var now = time.Now()
			var timestamp1 = now.Add(1000 * time.Millisecond)
			var timestamp2 = now.Add(2000 * time.Millisecond)
			var lww = New()

			var wg sync.WaitGroup
			wg.Add(2)
			go removeEl(&wg, &lww, "1", timestamp1)
			go addEl(&wg, &lww, "1", timestamp2)
			wg.Wait()

			var expected = []string{"1"}
			var results = lww.Get()
			if diff := cmp.Diff(expected, results); diff != "" {
				t.Fatalf("\t%s\tTest %d:\tShould get back correct lww set: [1]. Diff:\n%s", testFailed, testID, diff)
			}
			t.Logf("\t%s\tTest %d:\tShould get back correct lww set: [1].", testSuccess, testID)
		}

		testID = 6
		t.Logf("\tTest %d:\tWhen handling remove(1, 1), remove(1, 0).", testID)
		{
			var now = time.Now()
			var timestamp1 = now.Add(1000 * time.Millisecond)
			var timestamp2 = now.Add(2000 * time.Millisecond)
			var lww = New()

			var wg sync.WaitGroup
			wg.Add(2)
			go removeEl(&wg, &lww, "1", timestamp2)
			go removeEl(&wg, &lww, "1", timestamp1)
			wg.Wait()

			var expected []string
			var results = lww.Get()
			if diff := cmp.Diff(expected, results); diff != "" {
				t.Fatalf("\t%s\tTest %d:\tShould get back correct lww set: []. Diff:\n%s", testFailed, testID, diff)
			}
			t.Logf("\t%s\tTest %d:\tShould get back correct lww set: [].", testSuccess, testID)
		}

		testID = 7
		t.Logf("\tTest %d:\tWhen handling remove(1, 1), remove(1, 1).", testID)
		{
			var now = time.Now()
			var timestamp1 = now.Add(1000 * time.Millisecond)
			var lww = New()

			var wg sync.WaitGroup
			wg.Add(2)
			go removeEl(&wg, &lww, "1", timestamp1)
			go removeEl(&wg, &lww, "1", timestamp1)
			wg.Wait()

			var expected []string
			var results = lww.Get()
			if diff := cmp.Diff(expected, results); diff != "" {
				t.Fatalf("\t%s\tTest %d:\tShould get back correct lww set: []. Diff:\n%s", testFailed, testID, diff)
			}
			t.Logf("\t%s\tTest %d:\tShould get back correct lww set: [].", testSuccess, testID)
		}

		testID = 8
		t.Logf("\tTest %d:\tWhen handling remove(1, 1), remove(1, 2).", testID)
		{
			var now = time.Now()
			var timestamp1 = now.Add(1000 * time.Millisecond)
			var timestamp2 = now.Add(2000 * time.Millisecond)
			var lww = New()

			var wg sync.WaitGroup
			wg.Add(2)
			go removeEl(&wg, &lww, "1", timestamp1)
			go removeEl(&wg, &lww, "1", timestamp2)
			wg.Wait()

			var expected []string
			var results = lww.Get()
			if diff := cmp.Diff(expected, results); diff != "" {
				t.Fatalf("\t%s\tTest %d:\tShould get back correct lww set: []. Diff:\n%s", testFailed, testID, diff)
			}
			t.Logf("\t%s\tTest %d:\tShould get back correct lww set: [].", testSuccess, testID)
		}

		testID = 9
		t.Logf("\tTest %d:\tWhen handling add(1, 1), remove(1, 0).", testID)
		{
			var now = time.Now()
			var timestamp1 = now.Add(1000 * time.Millisecond)
			var timestamp2 = now.Add(2000 * time.Millisecond)
			var lww = New()

			var wg sync.WaitGroup
			wg.Add(2)
			go addEl(&wg, &lww, "1", timestamp2)
			go removeEl(&wg, &lww, "1", timestamp1)
			wg.Wait()

			var expected = []string{"1"}
			var results = lww.Get()
			if diff := cmp.Diff(expected, results); diff != "" {
				t.Fatalf("\t%s\tTest %d:\tShould get back correct lww set: [1]. Diff:\n%s", testFailed, testID, diff)
			}
			t.Logf("\t%s\tTest %d:\tShould get back correct lww set: [1].", testSuccess, testID)
		}

		testID = 10
		t.Logf("\tTest %d:\tWhen handling add(1, 1), remove(1, 1).", testID)
		{
			var now = time.Now()
			var timestamp1 = now.Add(1000 * time.Millisecond)
			var lww = New()

			var wg sync.WaitGroup
			wg.Add(2)
			go addEl(&wg, &lww, "1", timestamp1)
			go removeEl(&wg, &lww, "1", timestamp1)
			wg.Wait()

			var expected = []string{"1"}
			var results = lww.Get()
			if diff := cmp.Diff(expected, results); diff != "" {
				t.Fatalf("\t%s\tTest %d:\tShould get back correct lww set: [1]. Diff:\n%s", testFailed, testID, diff)
			}
			t.Logf("\t%s\tTest %d:\tShould get back correct lww set: [1].", testSuccess, testID)
		}

		testID = 11
		t.Logf("\tTest %d:\tWhen handling add(1, 1), remove(1, 2).", testID)
		{
			var now = time.Now()
			var timestamp1 = now.Add(1000 * time.Millisecond)
			var timestamp2 = now.Add(2000 * time.Millisecond)
			var lww = New()

			var wg sync.WaitGroup
			wg.Add(2)
			go addEl(&wg, &lww, "1", timestamp1)
			go removeEl(&wg, &lww, "1", timestamp2)
			wg.Wait()

			var expected []string
			var results = lww.Get()
			if diff := cmp.Diff(expected, results); diff != "" {
				t.Fatalf("\t%s\tTest %d:\tShould get back correct lww set: []. Diff:\n%s", testFailed, testID, diff)
			}
			t.Logf("\t%s\tTest %d:\tShould get back correct lww set: [].", testSuccess, testID)
		}
	}
}